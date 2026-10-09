# ChainStorage Component Diagram

## Overview

ChainStorage is a distributed blockchain data storage and processing system that continuously replicates blockchain data and serves it through APIs.

## High-Level Architecture (Simplified)

```mermaid
graph LR
    %% External
    BC[Blockchain<br/>Nodes] 
    ExtClients[External<br/>Clients]
    
    %% Core Components
    API[API Server]
    WF[Workflow<br/>Engine]
    BCC[Blockchain<br/>Clients]
    P[Parsers]
    
    %% Storage
    BS[Blob<br/>Storage]
    MS[Meta<br/>Storage]
    
    %% Flow
    ExtClients -->|REST/gRPC| API
    API --> BS
    API --> MS
    
    WF -->|Fetch| BCC
    BCC -->|Raw Data| BC
    BCC --> P
    P --> BS
    P --> MS
    
    %% Styling with Nord palette
    style BC fill:#d8dee9,stroke:#2e3440,stroke-width:2px,color:#2e3440
    style ExtClients fill:#d8dee9,stroke:#2e3440,stroke-width:2px,color:#2e3440
    style API fill:#3b4252,stroke:#2e3440,stroke-width:2px,color:#d8dee9
    style WF fill:#434c5e,stroke:#2e3440,stroke-width:2px,color:#d8dee9
    style BCC fill:#4c566a,stroke:#2e3440,stroke-width:2px,color:#d8dee9
    style P fill:#4c566a,stroke:#2e3440,stroke-width:2px,color:#d8dee9
    style BS fill:#3b4252,stroke:#2e3440,stroke-width:2px,color:#d8dee9
    style MS fill:#3b4252,stroke:#2e3440,stroke-width:2px,color:#d8dee9
```

## Detailed Architecture

```mermaid
graph TB
    %% External Systems
    subgraph EXT["🌐 External Systems"]
        BC[Blockchain Nodes<br/>Bitcoin/Ethereum/Solana/etc]
        Client[External Clients<br/>SDK/REST/gRPC]
        Temporal[Temporal Server]
        AWS[AWS Services<br/>S3/DynamoDB/SQS]
        GCP[GCP Services<br/>GCS/Firestore]
    end

    %% Entry Points
    subgraph ENTRY["🚀 Entry Points (cmd/)"]
        API[API Server<br/>REST/gRPC]
        Server[Main Server<br/>Workflow Manager]
        Worker[Worker<br/>Activity Executor]
        Admin[Admin CLI]
        Cron[Cron Jobs]
    end

    %% Storage Layer
    subgraph STORAGE["💾 Storage Layer"]
        BlobStorage[BlobStorage<br/>Raw Block Data]
        MetaStorage[MetaStorage<br/>Metadata & Indexes]
    end

    %% Blockchain Layer
    subgraph BLOCKCHAIN["⛓️ Blockchain Layer"]
        ClientFactory[Client Factory]
        subgraph CLIENTS["Multi-Endpoint Clients"]
            Master[Master Client<br/>Sticky Sessions]
            Slave[Slave Client<br/>Load Balanced]
            Validator[Validator Client]
            Consensus[Consensus Client]
        end
        
        subgraph PARSERS["Parsers"]
            NativeParser[Native Parser]
            RosettaParser[Rosetta Parser]
            TrustlessValidator[Trustless Validator]
        end
    end

    %% Workflow Engine
    subgraph WORKFLOW["⚙️ Workflow Engine"]
        subgraph WORKFLOWS["Workflows"]
            Backfiller[Backfiller<br/>Historical Blocks]
            Poller[Poller<br/>New Blocks]
            Streamer[Streamer<br/>Real-time]
            Monitor[Monitor<br/>Health Check]
            CrossValidator[Cross Validator]
            Replicator[Replicator]
        end
        
        subgraph ACTIVITIES["Activities"]
            Extractor[Extractor]
            Loader[Loader]
            Syncer[Syncer]
            Reader[Reader]
            EventLoader[Event Loader]
        end
    end

    %% Connections
    Client -->|REST/gRPC| API
    API -->|Read| BlobStorage
    API -->|Query| MetaStorage
    
    Server -->|Schedule| Temporal
    Worker -->|Execute| Temporal
    Temporal -->|Orchestrate| WORKFLOWS
    
    WORKFLOWS -->|Execute| ACTIVITIES
    ACTIVITIES -->|Use| ClientFactory
    ClientFactory -->|Create| Master
    ClientFactory -->|Create| Slave
    ClientFactory -->|Create| Validator
    ClientFactory -->|Create| Consensus
    
    Master -->|Fetch| BC
    Slave -->|Fetch| BC
    Validator -->|Validate| BC
    Consensus -->|Check| BC
    
    ACTIVITIES -->|Parse| NativeParser
    NativeParser -->|Convert| RosettaParser
    ACTIVITIES -->|Validate| TrustlessValidator
    
    ACTIVITIES -->|Store Raw| BlobStorage
    ACTIVITIES -->|Store Meta| MetaStorage
    BlobStorage -->|S3/GCS| AWS
    BlobStorage -->|S3/GCS| GCP
    MetaStorage -->|DynamoDB| AWS
    MetaStorage -->|Firestore| GCP
    
    Admin -->|Manual Ops| WORKFLOWS
    Admin -->|Direct Access| BlobStorage
    Admin -->|Direct Access| MetaStorage
    
    Cron -->|Scheduled| WORKFLOWS

    %% Styling with Nord palette
    classDef external fill:#d8dee9,stroke:#2e3440,stroke-width:2px,color:#2e3440
    classDef entry fill:#3b4252,stroke:#2e3440,stroke-width:2px,color:#d8dee9
    classDef storage fill:#434c5e,stroke:#2e3440,stroke-width:2px,color:#d8dee9
    classDef workflow fill:#4c566a,stroke:#2e3440,stroke-width:2px,color:#d8dee9
    classDef blockchain fill:#3b4252,stroke:#2e3440,stroke-width:2px,color:#d8dee9
    classDef subgraphStyle fill:#d8dee9,stroke:#2e3440,stroke-width:1px,color:#2e3440
    
    class BC,Client,Temporal,AWS,GCP external
    class API,Server,Worker,Admin,Cron entry
    class BlobStorage,MetaStorage storage
    class Backfiller,Poller,Streamer,Monitor,CrossValidator,Replicator,Extractor,Loader,Syncer,Reader,EventLoader workflow
    class ClientFactory,Master,Slave,Validator,Consensus,NativeParser,RosettaParser,TrustlessValidator blockchain
    class EXT,ENTRY,STORAGE,BLOCKCHAIN,WORKFLOW,WORKFLOWS,ACTIVITIES,CLIENTS,PARSERS subgraphStyle
```

## Component Breakdown

### Workflow Engine Detail

```mermaid
graph TB
    T[Temporal Server]
    
    subgraph Workflows
        BF[Backfiller]
        PL[Poller]
        ST[Streamer]
        MN[Monitor]
        CV[Cross Validator]
        RP[Replicator]
    end
    
    subgraph Activities
        EX[Extractor]
        LD[Loader]
        SY[Syncer]
        RD[Reader]
        EL[Event Loader]
    end
    
    T --> Workflows
    Workflows --> Activities
    
    style T fill:#d8dee9,stroke:#2e3440,stroke-width:2px,color:#2e3440
    style Workflows fill:#434c5e,stroke:#2e3440,stroke-width:2px,color:#d8dee9
    style Activities fill:#4c566a,stroke:#2e3440,stroke-width:2px,color:#d8dee9
```

### Blockchain Client Architecture

```mermaid
graph LR
    CF[Client Factory]
    
    subgraph Endpoints
        M[Master<br/>Sticky Sessions]
        S[Slave<br/>Load Balanced]
        V[Validator]
        C[Consensus]
    end
    
    subgraph Parsers
        NP[Native Parser]
        RP[Rosetta Parser]
        TV[Trustless Validator]
    end
    
    CF --> Endpoints
    Endpoints --> Parsers
    
    style CF fill:#3b4252,stroke:#2e3440,stroke-width:2px,color:#d8dee9
    style Endpoints fill:#434c5e,stroke:#2e3440,stroke-width:2px,color:#d8dee9
    style Parsers fill:#4c566a,stroke:#2e3440,stroke-width:2px,color:#d8dee9
```

### Storage Architecture

```mermaid
graph TB
    subgraph BlobStorage
        S3[AWS S3]
        GCS[Google Cloud Storage]
    end
    
    subgraph MetaStorage
        DDB[DynamoDB]
        FS[Firestore]
    end
    
    BS[Blob Storage<br/>Interface] --> S3
    BS --> GCS
    
    MS[Meta Storage<br/>Interface] --> DDB
    MS --> FS
    
    style BlobStorage fill:#434c5e,stroke:#2e3440,stroke-width:2px,color:#d8dee9
    style MetaStorage fill:#434c5e,stroke:#2e3440,stroke-width:2px,color:#d8dee9
    style BS fill:#3b4252,stroke:#2e3440,stroke-width:2px,color:#d8dee9
    style MS fill:#3b4252,stroke:#2e3440,stroke-width:2px,color:#d8dee9
```

## Component Details

### Entry Points

1. **API Server** (`cmd/api`)
   - Serves REST and gRPC endpoints
   - Provides block data, events, and metadata
   - Handles authentication and rate limiting

2. **Main Server** (`cmd/server`)
   - Manages Temporal workflows
   - Coordinates blockchain data ingestion
   - Handles workflow scheduling

3. **Worker** (`cmd/worker`)
   - Executes Temporal activities
   - Performs actual blockchain data fetching
   - Handles parsing and validation

4. **Admin CLI** (`cmd/admin`)
   - Manual workflow management
   - Direct storage operations
   - Debugging and maintenance

5. **Cron** (`cmd/cron`)
   - Scheduled maintenance tasks
   - Periodic health checks
   - Cleanup operations

### Storage Layer

1. **BlobStorage**
   - Stores compressed raw block data
   - Supports S3 (AWS) and GCS (GCP)
   - Provides presigned URLs for direct access
   - Handles block versioning

2. **MetaStorage**
   - Stores block metadata (height, hash, timestamp)
   - Maintains transaction indexes
   - Tracks events (block added/removed)
   - Supports DynamoDB (AWS) and Firestore (GCP)

### Blockchain Layer

1. **Client System**
   - **Master**: Primary endpoint with sticky sessions
   - **Slave**: Load-balanced endpoints for ingestion
   - **Validator**: Data validation endpoints
   - **Consensus**: Consensus verification endpoints

2. **Parser System**
   - **Native Parser**: Blockchain-specific parsing
   - **Rosetta Parser**: Standardized Rosetta format
   - **Trustless Validator**: Cryptographic validation

### Workflow Engine

1. **Core Workflows**
   - **Backfiller**: Historical block ingestion
   - **Poller**: Continuous new block polling
   - **Streamer**: Real-time block streaming
   - **Monitor**: System health monitoring
   - **Cross Validator**: Cross-chain validation
   - **Replicator**: Data replication

2. **Activities**
   - **Extractor**: Fetches raw blocks
   - **Loader**: Stores processed blocks
   - **Syncer**: Synchronizes blockchain state
   - **Reader**: Reads stored data
   - **Event Loader**: Processes blockchain events

## Data Flow

1. **Ingestion Flow**
   ```
   Blockchain Node → Client → Parser → Validator → Storage
   ```

2. **Query Flow**
   ```
   External Client → API Server → Storage → Response
   ```

3. **Workflow Flow**
   ```
   Temporal → Workflow → Activity → Client → Blockchain
                    ↓
                Storage
   ```

## Key Design Patterns

- **Factory Pattern**: Dynamic blockchain client/parser creation
- **Interceptor Pattern**: Request/response instrumentation
- **Module Pattern**: Clean separation of concerns
- **Dependency Injection**: Using Uber FX framework
- **Option Pattern**: Flexible configuration

## Supported Blockchains

- **EVM**: Ethereum, Polygon, BSC, Arbitrum, Optimism, Base, Fantom, Avalanche
- **Bitcoin-based**: Bitcoin, Bitcoin Cash, Dogecoin, Litecoin
- **Others**: Solana, Aptos, Tron
- **Special**: Ethereum Beacon Chain

## Scalability Features

- Horizontal scaling via distributed storage
- Multi-endpoint load balancing
- Temporal workflow orchestration
- Configurable batch processing
- Support for 1,500+ blocks/second