package main

import (
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Endpoint           string   `yaml:"endpoint"`
	Protocol           string   `yaml:"protocol"`
	Framing            string   `yaml:"framing"`
	MessagesPerSecond  int      `yaml:"messages_per_second"`
	Workers            int      `yaml:"workers"`
	Duration           string   `yaml:"duration"`
	MaxMessages        int64    `yaml:"max_messages"`
	Messages           []string `yaml:"messages"`
}

func (c *Config) duration() time.Duration {
	if c.Duration == "" || c.Duration == "0s" {
		return 0
	}
	d, err := time.ParseDuration(c.Duration)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid duration %q: %v\n", c.Duration, err)
		os.Exit(1)
	}
	return d
}

var syslogTimestampRe = regexp.MustCompile(`^<\d+>\d+ (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[^\s]+)\s`)

func replaceTimestamp(msg string) string {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	loc := syslogTimestampRe.FindStringSubmatchIndex(msg)
	if loc == nil {
		return msg
	}
	var b strings.Builder
	b.WriteString(msg[:loc[2]])
	b.WriteString(now)
	b.WriteString(msg[loc[3]:])
	return b.String()
}

func formatCount(n int64) string {
	switch {
	case n >= 1_000_000_000:
		return fmt.Sprintf("%.1fB", float64(n)/1_000_000_000)
	case n >= 1_000_000:
		return fmt.Sprintf("%.1fM", float64(n)/1_000_000)
	case n >= 1_000:
		return fmt.Sprintf("%.1fk", float64(n)/1_000)
	default:
		return fmt.Sprintf("%d", n)
	}
}

func formatBytes(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1fGiB", float64(n)/float64(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1fMiB", float64(n)/float64(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1fKiB", float64(n)/float64(1<<10))
	default:
		return fmt.Sprintf("%dB", n)
	}
}

type Stats struct {
	sent   atomic.Int64
	errors atomic.Int64
	bytes  atomic.Int64
}

func frameMessage(msg string, framing string) []byte {
	switch framing {
	case "newline":
		return []byte(msg + "\n")
	case "octet-counting":
		return []byte(strconv.Itoa(len(msg)) + " " + msg)
	default:
		return []byte(msg)
	}
}

func worker(conn net.Conn, cfg *Config, stats *Stats, stop <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	defer conn.Close()

	interval := time.Second / time.Duration(cfg.MessagesPerSecond/cfg.Workers)
	if interval == 0 {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			msg := replaceTimestamp(cfg.Messages[rand.Intn(len(cfg.Messages))])
			data := frameMessage(msg, cfg.Framing)
			_, err := conn.Write(data)
			if err != nil {
				stats.errors.Add(1)
				fmt.Fprintf(os.Stderr, "send error: %v\n", err)
				return
			}
			stats.sent.Add(1)
			stats.bytes.Add(int64(len(data)))
		}
	}
}

func connect(cfg *Config) (net.Conn, error) {
	switch cfg.Protocol {
	case "tcp":
		return net.DialTimeout("tcp", cfg.Endpoint, 5*time.Second)
	case "udp":
		return net.DialTimeout("udp", cfg.Endpoint, 5*time.Second)
	default:
		return nil, fmt.Errorf("unsupported protocol: %s", cfg.Protocol)
	}
}

func run(cfg *Config) {
	stats := &Stats{}
	stop := make(chan struct{})
	var wg sync.WaitGroup

	perWorker := cfg.MessagesPerSecond / cfg.Workers
	if perWorker == 0 {
		perWorker = 1
	}
	effectiveRate := perWorker * cfg.Workers
	fmt.Printf("starting syslog generator: endpoint=%s protocol=%s framing=%s rate=%d/msg/s workers=%d\n",
		cfg.Endpoint, cfg.Protocol, cfg.Framing, effectiveRate, cfg.Workers)

	for i := 0; i < cfg.Workers; i++ {
		conn, err := connect(cfg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "worker %d connect error: %v\n", i, err)
			continue
		}
		workerCfg := *cfg
		workerCfg.MessagesPerSecond = perWorker
		wg.Add(1)
		go worker(conn, &workerCfg, stats, stop, &wg)
	}

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				sent := stats.sent.Load()
				errs := stats.errors.Load()
				b := stats.bytes.Load()
				fmt.Printf("[stats] sent=%s errors=%s bytes=%s\n",
					formatCount(sent), formatCount(errs), formatBytes(b))
			}
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)

	if dur := cfg.duration(); dur > 0 {
		select {
		case <-sigCh:
		case <-time.After(dur):
		}
	} else {
		maxReached := make(chan struct{})
		if cfg.MaxMessages > 0 {
			go func() {
				for stats.sent.Load() < cfg.MaxMessages {
					time.Sleep(100 * time.Millisecond)
				}
				close(maxReached)
			}()
			select {
			case <-sigCh:
			case <-maxReached:
			}
		} else {
			<-sigCh
		}
	}

	close(stop)
	wg.Wait()
	printSummary(cfg, stats)
}

func printSummary(cfg *Config, stats *Stats) {
	sent := stats.sent.Load()
	errs := stats.errors.Load()
	b := stats.bytes.Load()
	fmt.Println("--- summary ---")
	fmt.Printf("  messages sent:  %s\n", formatCount(sent))
	fmt.Printf("  errors:         %s\n", formatCount(errs))
	fmt.Printf("  bytes sent:     %s\n", formatBytes(b))
	fmt.Printf("  protocol:       %s\n", cfg.Protocol)
	fmt.Printf("  framing:        %s\n", cfg.Framing)
	fmt.Printf("  endpoint:       %s\n", cfg.Endpoint)
	fmt.Printf("  workers:        %d\n", cfg.Workers)
}

func loadConfig(path string) *Config {
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to read config: %v\n", err)
		os.Exit(1)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		fmt.Fprintf(os.Stderr, "failed to parse config: %v\n", err)
		os.Exit(1)
	}
	return &cfg
}

func main() {
	configPath := flag.String("config", "config.yaml", "path to config file")
	endpoint := flag.String("endpoint", "", "override endpoint (host:port)")
	protocol := flag.String("protocol", "", "override protocol (tcp|udp)")
	framing := flag.String("framing", "", "override framing (none|newline|octet-counting)")
	rate := flag.Int("rate", 0, "override messages per second")
	workers := flag.Int("workers", 0, "override number of workers")
	duration := flag.String("duration", "", "override duration (e.g. 60s, 5m)")
	maxMessages := flag.Int64("max-messages", 0, "override max messages (0=unlimited)")
	flag.Parse()

	cfg := loadConfig(*configPath)

	if *endpoint != "" {
		cfg.Endpoint = *endpoint
	}
	if *protocol != "" {
		cfg.Protocol = *protocol
	}
	if *framing != "" {
		cfg.Framing = *framing
	}
	if *rate > 0 {
		cfg.MessagesPerSecond = *rate
	}
	if *workers > 0 {
		cfg.Workers = *workers
	}
	if *duration != "" {
		cfg.Duration = *duration
	}
	if *maxMessages > 0 {
		cfg.MaxMessages = *maxMessages
	}

	if cfg.MessagesPerSecond <= 0 {
		fmt.Fprintln(os.Stderr, "messages_per_second must be > 0")
		os.Exit(1)
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 1
	}
	if cfg.Protocol == "" {
		cfg.Protocol = "udp"
	}
	if cfg.Framing == "" {
		cfg.Framing = "none"
	}
	switch cfg.Framing {
	case "none", "newline", "octet-counting":
	default:
		fmt.Fprintf(os.Stderr, "unsupported framing: %s (must be none, newline, or octet-counting)\n", cfg.Framing)
		os.Exit(1)
	}
	if cfg.Endpoint == "" {
		fmt.Fprintln(os.Stderr, "endpoint is required")
		os.Exit(1)
	}

	run(cfg)
}