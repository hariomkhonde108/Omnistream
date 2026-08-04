# OmniStream

A distributed, event-driven real-time collaboration platform featuring resumable chunked file sharing, WebRTC conferencing. 

Built with Go, Kafka, PostgreSQL, and MinIO, OmniStream is designed to handle high-throughput I/O without blocking the main API or exhausting server memory.

##  System Architecture

![OmniStream Architecture Diagram](https://github.com/hariomkhonde108/Omnistream/blob/main/omniStream_architecture.png)  
*(Add your Eraser.io architecture diagram here)*

##  Key Features

* **Resumable Chunked Uploads:** Capable of handling massive file uploads over unstable networks. If a connection drops, clients only retry the missing chunks.
* **Zero-Buffer Ranged Downloads:** Streams gigabytes of data directly from object storage to the client over HTTP Range requests with an $O(1)$ memory footprint (~32 KB).
* **Real-Time Push Notifications:** Push-only WebSocket hub ensures connected clients are instantly notified when async file processing or AI tasks complete.
* **Multi-Peer Delivery Tracking:** Safe, idempotent database design ensures file delivery state is tracked accurately per participant, even for late joiners.

## Microservices Breakdown

To ensure high availability and prevent resource starvation, the backend is decoupled into four dedicated Go binaries:

1. **`cmd/api` (The Synchronous Edge):** Manages room lifecycles, authentication, WebSocket fan-out, LiveKit webhook verification, and zero-buffer file download streaming.
2. **`cmd/ingestion` (The I/O Engine):** A dedicated service handling the resumable chunked upload protocol. Streams bytes directly to MinIO and manages state in PostgreSQL to protect the main API from network saturation.
3. **`cmd/worker` (The Event Processor):** Consumes Kafka events for standard file uploads, updates relational state, and triggers real-time WebSocket broadcasts.



## Tech Stack

* **Language:** Go (Golang)
* **Messaging:** Apache Kafka (Segmentio `kafka-go`)
* **Database:** PostgreSQL (`pgxpool`)
* **Object Storage:** MinIO (S3-compatible)
* **Real-Time:** Gorilla WebSockets, LiveKit (WebRTC)
* **API Framework:** Gin
