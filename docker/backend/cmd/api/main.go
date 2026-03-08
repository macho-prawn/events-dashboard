package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/macho_prawn/events-dashboard/internal/api"
	"github.com/macho_prawn/events-dashboard/internal/auth"
	"github.com/macho_prawn/events-dashboard/internal/config"
	"github.com/macho_prawn/events-dashboard/internal/store"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	eventStore, err := store.NewPostgresStore(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("open database: %v", err)
	}

	if err := eventStore.AutoMigrate(); err != nil {
		log.Fatalf("migrate database: %v", err)
	}

	accessConfig, err := eventStore.EnsureAPIKeyAccess(context.Background(), cfg.DefaultAPIKeyAccess())
	if err != nil {
		log.Fatalf("initialize api_key_access config: %v", err)
	}

	keyManager, err := auth.NewManager(
		accessConfig.IngestionSigningSecret,
		accessConfig.IngestionIssuer,
		accessConfig.IngestionSubject,
		time.Duration(accessConfig.IngestionTTLSeconds)*time.Second,
	)
	if err != nil {
		log.Fatalf("initialize key manager: %v", err)
	}

	accessKeyManager, err := auth.NewManager(accessConfig.AccessSigningSecret, accessConfig.AccessIssuer, accessConfig.AccessSubject, 0)
	if err != nil {
		log.Fatalf("initialize access key manager: %v", err)
	}

	publicHandler, err := api.NewPublicServer(api.Dependencies{
		Store:            eventStore,
		KeyManager:       keyManager,
		AccessKeyManager: accessKeyManager,
	})
	if err != nil {
		log.Fatalf("initialize public API server: %v", err)
	}

	publicServer := &http.Server{
		Addr:              cfg.PublicServerAddr,
		Handler:           publicHandler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("public HTTP server listening on %s", cfg.PublicServerAddr)
		if err := publicServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("public listen and serve: %v", err)
		}
	}()

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := publicServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("public graceful shutdown failed: %v", err)
	}

	if err := eventStore.Close(); err != nil {
		log.Printf("close database: %v", err)
	}
}
