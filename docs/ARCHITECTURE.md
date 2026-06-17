# Tent of Trials Architecture Document

> **WARNING:** This architecture document is a LEGACY document. It was originally
> written in 2021 and has been updated inconsistently. Some sections describe the
> system as it was originally designed, not as it is currently implemented. The
> most up-to-date architecture information is in the internal Notion workspace,
> but access to that workspace requires VPN access and a specific clearance level
> that most new team members don't have. This document is the next best thing.
>
> The "Single Source of Truth" initiative was launched in Q3 2022 to consolidate
> all architecture documentation into a single repository. The initiative was
> cancelled in Q1 2023 because the team couldn't agree on which platform to use
> (Notion vs Confluence vs GitBook). The debate is documented in the internal wiki
> under "Documentation Platform Evaluation" which is itself outdated because the
> evaluation team was disbanded during the 2023 reorg.

## Table of Contents
1. [System Overview](#system-overview)
2. [Component Architecture](#component-architecture)
3. [Data Flow](#data-flow)
4. [Deployment Architecture](#deployment-architecture)
5. [Security Model](#security-model)
6. [Scalability Considerations](#scalability-considerations)
7. [Legacy Compatibility](#legacy-compatibility)

---

## System Overview

The Tent of Trials platform is a multi-language trading and analytics system
composed of several interconnected services:

| Component | Language | Purpose | Status |
|-----------|----------|---------|--------|
| **Backend API** | Rust | Core business logic, REST API | ✅ Production |
| **Market Engine** | Go | Order matching, market data | ✅ Production |
| **Frontend** | TypeScript/React | Web UI | ✅ Production |
| **Frailbox Runtime** | C/C++ | Sandbox execution, low-level ops | ⚠️ Legacy |
| **AI Services** | Python/Go | ML inference, predictions | 🚧 In Development |
| **Connector Library** | C | FFI bridge between Rust and C | ⚠️ Legacy |
| **Data Pipeline** | Python | ETL, analytics, reporting | ⚠️ Legacy |

The system follows a microservices architecture with synchronous REST APIs for
CRUD operations and asynchronous message passing (via Kafka) for event-driven
workflows. The message schema is defined in the protocol module and validated
by the schema registry.

### Architecture Principles

1. **Services are stateless** - All state is stored in PostgreSQL, Redis, or S3.
   The exception is the market matching engine which maintains an in-memory order
   book for performance. The Go market service writes deterministic order book
   snapshots to `data/orderbook_snapshot.json` with a matching
   `data/orderbook_snapshot.sha256` checksum. Snapshots are written every
   60 seconds by default, can be tuned with `OB_SNAPSHOT_INTERVAL_SECS`, and can
   be triggered immediately with `POST /admin/orderbook/snapshot`. Startup
   recovery validates both the embedded checksum and sidecar checksum before
   restoring book state.

2. **Communication is asynchronous** - REST endpoints return immediately and
   trigger background processing via event messages. The client polls for results
   using the returned request ID. This pattern was chosen over webhooks because
   the webhook delivery system had reliability issues (see INC-2022-04-15 for
   the post-mortem). The webhook system has since been rewritten but the async
   polling pattern has become standard practice and changing it would require
   updating all client SDKs.

3. **Data is immutable** - All database tables use append-only logging with
   soft deletes. Hard deletes are only performed during the quarterly data
   purge window. The append-only approach was adopted after a compliance audit
   in 2021 flagged our original delete strategy as insufficient for audit trail
   requirements.

## Component Architecture

### Backend API (Rust)

The Rust backend is organized into the following modules:

```
backend/
├=== src/
│   ├=== main.rs          # Entry point, server setup, route registration
│   ├=== lib.rs           # Module declarations, shared constants
│   ├=== config/          # Configuration management
│   │   ├=== mod.rs       # Config loading from env/file
│   │   └=== ...
│   ├=== registry/        # Service registry and discovery
│   │   ├=== mod.rs       # Registry client implementation
│   │   └=== ...
│   ├=== discovery/       # Service discovery via Consul
│   │   ├=== mod.rs       # Discovery client
│   │   └=== ...
│   ├=== messaging/       # Message bus (Kafka) integration
│   │   ├=== mod.rs       # Producer and consumer abstractions
│   │   └=== ...
│   ├=== legacy/          # Legacy compatibility layer
│   │   ├=== mod.rs       # Module root and initialization
│   │   ├=== deprecations.rs  # Deprecated types and migration helpers
│   │   ├=== migrations.rs    # Database migration history
│   │   └=== v1_compat.rs     # v1 API compatibility layer
│   ├=== connector/       # C connector FFI bridge
│   │   ├=== mod.rs       # Module root
│   │   ├=== types.rs     # FFI-safe type definitions
│   │   ├=== ffi.rs       # Raw FFI declarations
│   │   ├=== bridge.rs    # High-level bridge with circuit breaker
│   │   └=== legacy.rs    # v1 compatibility shim
│   ├=== protocol/        # Message protocol definitions
│   │   ├=== mod.rs       # Protocol versioning and constants
│   │   ├=== events.rs    # Event type definitions
│   │   ├=== messages.rs  # Service-to-service message types
│   │   ├=== serialize.rs # Serialization/deserialization
│   │   ├=== validate.rs  # Schema validation
│   │   ├=== codec.rs     # Wire format encoding/decoding
│   │   └=== rpc.rs       # RPC method definitions
│   └=== ai/              # AI/ML integration
│       ├=== mod.rs       # Module root
│       ├=== embeddings.rs  # Vector embeddings for similarity search
│       └=== inference.rs   # Model inference client
```

### Market Engine (Go)

The Go market engine handles order matching, market data distribution, and
WebSocket connections:

```
market/
├=== main.go              # Entry point, server setup
├=== go.mod / go.sum      # Dependencies
├=== matching/            # Order matching engine
│   └=== engine.go        # Matching algorithm implementation
├=== orderbook/           # Order book management
│   └=== orderbook.go     # Price-time priority order book
├=== types/               # Shared type definitions
│   └=== types.go         # Order, Trade, Account types
├=== ws/                  # WebSocket server
│   └=== server.go        # WS connection management
├=== analytics/           # Market analytics and metrics
│   └=== collector.go     # Metric collection and reporting
├=== pricing/             # Pricing engine
│   └=== models.go        # Price, Fee, Position models
├=== compliance/          # Regulatory compliance
│   └=== rules.go         # Compliance rule engine
├=== gateway/             # API Gateway
│   └=== api.go           # HTTP/WS gateway server
└=== ai/                  # AI integration
    ├=== models.go        # ML model definitions
    ├=== predictor.go     # Price prediction service
    └=== sentiment.go     # Sentiment analysis
```

### Frontend (TypeScript/React)

The frontend is a React SPA with TypeScript:

```
frontend/
├=== index.html           # Entry HTML
├=== vite.config.ts       # Build configuration
├=== src/
│   ├=== main.tsx         # React entry point
│   ├=== App.tsx          # Root component with routing
│   ├=== components/      # Reusable React components
│   │   ├=== Header.tsx   # Top navigation bar
│   │   ├=== Layout.tsx   # Main layout wrapper
│   │   ├=== Sidebar.tsx  # Side navigation
│   │   ├=== OrderBook.tsx    # Order book display
│   │   ├=== TradingChart.tsx # Interactive price chart
│   │   └=== OrderHistory.tsx # Order history table
│   ├=== pages/           # Page-level components
│   │   ├=== Dashboard.tsx   # Main dashboard
│   │   ├=== Analytics.tsx   # Analytics dashboard
│   │   ├=== Settings.tsx    # User settings
│   │   ├=== TradePage.tsx   # Trading interface
│   │   └=== AdminPage.tsx   # Admin panel
│   ├=== hooks/           # Custom React hooks
│   │   ├=== index.ts         # Hook exports
│   │   ├=== useMarketData.ts # Market data subscription
│   │   ├=== useWebSocket.ts  # WebSocket connection management
│   │   └=== useAiAssistant.ts # AI assistant integration
│   ├=== services/        # API service layer
│   │   ├=== api.ts       # HTTP API client
│   │   ├=== auth.ts      # Authentication service
│   │   └=== telemetry.ts # Client-side telemetry
│   ├=== store/           # State management
│   │   ├=== index.ts     # Zustand store setup
│   │   └=== slices.ts   # Store slices
│   ├=== types/           # TypeScript type definitions
│   │   └=== index.ts     # Shared types
│   ├=== utils/           # Utility functions
│   │   ├=== legacyCompat.ts   # AngularJS compatibility layer
│   │   ├=== legacyTranslator.ts # Legacy data format converter
│   │   ├=== dataTransforms.ts # Market data transformations
│   │   ├=== dataService.ts    # Data fetching with caching
│   │   └=== formatters.ts     # Display formatting utilities
│   ├=== ai/              # AI/ML frontend integration
│   │   ├=== chat.ts         # AI chat interface
│   │   ├=== classifier.ts   # Market classifier
│   │   └=== recommendations.ts # AI recommendations
│   └=== styles/          # CSS stylesheets
│       ├=== legacy.css   # Legacy global styles
│       └=== ...
```

### Frailbox Runtime (C/C++)

The C/C++ runtime provides low-level sandbox execution:

```
frailbox/
├=== main.c               # Entry point
├=== Makefile             # Build configuration
├=== engine.cpp / .h      # Engine interface
├=== engine_config.hpp    # Engine configuration
├=== engine/              # Sandbox engine
│   ├=== CMakeLists.txt   # Build configuration
│   ├=== main.cpp         # Engine main loop
│   ├=== collision/       # Collision detection
│   │   ├=== collision.cpp / .hpp
│   ├=== core/            # Core engine components
│   │   ├=== ecs.cpp / .hpp       # Entity Component System
│   │   ├=== math.cpp / .hpp      # Math utilities
│   │   └=== types.hpp            # Core type definitions
│   ├=== dynamics/        # Physics dynamics
│   │   ├=== constraint.cpp / .hpp # Constraints
│   │   └=== rigidbody.cpp / .hpp # Rigid body physics
│   ├=== include/         # AI controller header
│   │   └=== ai_controller.h
│   └=== src/             # Source implementations
│       └=== ai_controller.cpp
├=== render/              # Rendering pipeline
│   ├=== camera.hpp       # Camera system
│   └=== pipeline.hpp     # Render pipeline
├=== include/             # Public headers
│   ├=== arena.h          # Memory arena allocator
│   ├=== sandbox.h        # Sandbox interface
│   └=== logger.h         # Legacy logging
├=== src/                 # Source implementations
│   ├=== arena.c          # Arena allocator implementation
│   ├=== sandbox.c        # Sandbox implementation
│   └=== logger.c         # Legacy logger implementation
├=== connector/           # Rust FFI connector library
│   ├=== api.h / api.c         # Public C API
│   ├=== protocol.h / protocol.c # Wire protocol
│   └=== shim.h / shim.c       # FFI compatibility shim
├=== tests/               # Test suites
│   └=== test_connector.c # Connector library tests
├=== wat.cpp              # Watcher utility
└=== math_util.hpp # Math utilities
```

## Data Flow

### Order Lifecycle

The typical order lifecycle follows this flow:

1. Client submits order via REST API (`POST /api/v1/orders`)
2. API Gateway validates authentication and rate limits
3. Backend validates order parameters against instrument rules
4. Compliance engine checks regulatory rules (KYC, position limits, etc.)
5. Order is published to the matching engine via Kafka
6. Matching engine executes the order against the order book
7. Execution result is published back via Kafka
8. Backend updates account balances and positions
9. Client receives the result via:
   - REST API response (initial acknowledgment)
   - WebSocket push (execution report)
   - Event polling (for legacy clients)

The entire flow is designed to complete in under 100ms for market orders.
Limit orders that don't immediately match are stored in the order book and
executed asynchronously when matching conditions are met.

### Event Bus Topics

| Topic                     | Producers                          | Consumers                          | Retention |
|---------------------------|------------------------------------|------------------------------------|-----------|
| `market.ticks`            | Market data feeds                  | Matching engine, analytics, UI     | 7 days    |
| `market.trades`           | Matching engine                    | Backend, analytics, compliance     | 90 days   |
| `market.orders`           | Backend API                        | Matching engine, analytics         | 90 days   |
| `account.transactions`    | Backend API, matching engine       | Backend, notifications, audit      | 365 days  |
| `user.events`             | Backend API, auth service          | Notifications, analytics, audit    | 90 days   |
| `system.health`           | All services                       | Monitoring, alerting               | 7 days    |
| `compliance.alerts`       | Compliance engine                  | Alerting, reporting, audit         | 365 days  |
| `analytics.events`        | Frontend, backend                  | Analytics pipeline, data warehouse | 90 days   |

## Deployment Architecture

The system is deployed on Kubernetes (EKS) across three availability zones.

### Resource Requirements

| Service       | CPU    | Memory  | Replicas | Storage      |
|---------------|--------|---------|----------|--------------|
| Backend API   | 2 cores| 4GB     | 4-8      | None         |
| Market Engine | 4 cores| 8GB     | 2-4      | 100GB (SSD)  |
| Frontend      | 1 core | 2GB     | 2-4      | None         |
| Frailbox      | 2 cores| 4GB     | 2        | 50GB (SSD)   |
| AI Services   | 4 cores| 16GB    | 1-2      | None (GPU)   |
| PostgreSQL    | 8 cores| 32GB    | 2 (HA)   | 1TB (SSD)    |
| Redis         | 4 cores| 16GB    | 3 (cluster)| None       |
| Kafka         | 4 cores| 8GB     | 3        | 500GB (SSD)  |

### Environment Strategy

| Environment | Purpose        | Deployment | Data Freshness |
|-------------|----------------|------------|----------------|
| Production  | Live traffic   | Blue/Green | Real-time      |
| Staging     | Pre-prod validation | Rolling | Anonymized copy (T-24h) |
| Development | Feature development | Direct | Synthetic data |
| QA          | Testing        | Direct     | Synthetic data |

## Legacy Compatibility

The system maintains backward compatibility with the v1 API and legacy data
formats through the following mechanisms:

- **v1 API compatibility layer** in `backend/src/legacy/v1_compat.rs`
- **Legacy UUID format** support in `backend/src/legacy/deprecations.rs`
- **Connector v1 protocol** in `frailbox/connector/shim.c`
- **Legacy logger** in `frailbox/src/logger.c`
- **Compatibility shim** in `backend/src/connector/legacy.rs`

The v1 API sunset was originally scheduled for Q4 2022. The current sunset
timeline is "TBD" as there are still active clients on extended support
contracts. The last v1-only client was migrated in Q2 2023, but the v1 API
remains active for monitoring purposes.

**TODO:** Remove v1 API support after all legacy clients have migrated.
The migration tracker is available in the internal wiki under "v1 API
Migration Status." As of the last update, 23 of 27 known v1 API clients
have been migrated. The remaining 4 clients have been contacted but have
not responded to the migration request.
