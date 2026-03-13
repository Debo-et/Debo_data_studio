[file name]: README.md
[file content begin]
# Enterprise Database Backend Service

A robust backend service for database metadata inspection with built-in PostgreSQL connection management.

## Features

- 🐘 **Automatic PostgreSQL Connection**: Robust connection logic with retry and error handling
- 🔌 **Multiple Database Support**: MySQL, PostgreSQL, Oracle, SQL Server, DB2, SAP HANA, Sybase, Netezza, Informix, Firebird
- 📊 **Schema Inspection**: Comprehensive metadata extraction from all supported databases
- 🛡️ **Security**: Helmet.js, CORS, rate limiting
- 🏗️ **Connection Pooling**: Efficient connection management
- 📈 **Health Monitoring**: Built-in health checks and metrics
- 🐳 **Docker Support**: Ready-to-run PostgreSQL container

## Prerequisites

- Node.js 16+
- PostgreSQL 14+ (for local backend database)
- TypeScript 5+

## Quick Start

### 1. Clone and Install

```bash
git clone <repository-url>
cd enterprise-db-backend
npm install