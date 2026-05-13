# Talend‑Inspired SQL‑Based Data Integration Platform
Visual data pipeline design meets the power of PostgreSQL Foreign Data Wrappers

---

## 📖 Overview

This project is a **React + TypeScript** application that lets you design data integration pipelines through a **graphical node‑based canvas**, inspired by the workflow of **Talend Open Studio**.  
However, instead of generating Java code, our system dynamically produces **SQL statements** – specifically `INSERT ... SELECT` queries – that move and transform data directly inside **PostgreSQL**, using **Foreign Data Wrappers (FDW)** and **foreign tables**.

The result is a lightweight, database‑centric ETL tool that leverages PostgreSQL’s mature query engine and eliminates the need for intermediate application‑layer processing.

---

## 🚀 Getting Started (Web Mode)

This guide helps you build and run the application as a web‑based tool (browser only). The backend runs as a Node.js server and the frontend is served by Vite.

### Prerequisites

- **Node.js** (v18.0.0 or later) – check with `node -v`
- **npm** (v8.0.0 or later) – included with Node.js
- **PostgreSQL** (v14 or later) – running locally with a database (default `postgres`)
- **Git** – to clone the repository
- **Visual Studio** (optional) – you can use **Visual Studio Code** (recommended) or **Visual Studio 2022** with the *Node.js development* workload installed.

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/debo-data-studio.git
cd debo-data-studio

Install Dependencies

Install dependencies for the root, backend, and client:

# Root (dev scripts, Electron builder, etc.)
npm install

# Backend
cd backend
npm install
cd ..

# Client (frontend)
cd client
npm install
cd ..


3. Configure PostgreSQL

Make sure PostgreSQL is running and accessible. The backend expects a local PostgreSQL instance with the following default credentials (can be changed via environment variables):

    Host: localhost

    Port: 5432

    Database: postgres

    User: your system user (or postgres)

    Password: (may be empty)

You can override these by setting environment variables before starting the backend:

    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

4. Run in Development Mode

The easiest way is to use the root‑level script that starts both backend and frontend automatically:

npm run dev:full

This command:

    Starts the backend (Express server) on port 3000

    Waits for the /health endpoint to become healthy

    Starts the frontend dev server (Vite) on port 3001

    Opens your browser to http://localhost:3001

Alternatively, you can run them separately in two terminals:

Terminal 1 – Backend:

cd backend
npm run dev

Terminal 2 – Frontend:

cd client
npm run dev

5. Using Visual Studio
Visual Studio Code (recommended)

    Open the project folder (debo-data-studio) in VS Code.

    Open the integrated terminal (Ctrl + `).

    Run the commands from step 4 above.

    VS Code will automatically recognise the TypeScript configuration and provide intelligent code completion.

Visual Studio 2022 (with Node.js workload)

    Open Visual Studio 2022.

    Click File → Open → Folder… and select the debo-data-studio folder.

    Visual Studio will detect the package.json files and may ask to restore npm packages – allow it.

    Open the Solution Explorer to see the folder structure.

    Open the Package Manager Console (or a terminal window) from View → Terminal.

    Run the following commands (one after the other):

    # Ensure dependencies are installed
npm install
cd backend; npm install; cd ..
cd client; npm install; cd ..

Start the application using the integrated terminal:

npm run dev:full

    The browser will open automatically with the app.

    Tip: You can also create launch.json configurations for debugging both processes, but for development the dev:full script is sufficient.

    6. Building for Production (Web Mode)

To create a production build of the frontend and run the backend in production mode:

# Build frontend
cd client
npm run build
cd ..

# Build backend (if needed, it compiles TypeScript to JavaScript)
cd backend
npm run build
cd ..

Now you can serve the frontend static files (from client/dist) using any static web server, and start the backend with:

cd backend
NODE_ENV=production node dist/server.js

🏗️ Architecture

+---------------------+ +------------------------+
| React Frontend | | PostgreSQL Backend |
| (Canvas, Sidebar, | |------------------------|
| Right Panel) | | - FDW extensions |
+---------------------+ | - Foreign tables |
| | | - Generated SQL |
| v +------------------------+
+---------------------+
| SQL Generator |
| (TypeScript module) |
+---------------------+

    Frontend: React with TypeScript, React Flow for the canvas, Redux for state.

    Backend: PostgreSQL (≥ 12) with appropriate FDW extensions (e.g., file_fdw, postgres_fdw, mysql_fdw, ogr_fdw).

    SQL Generation: A dedicated TypeScript module that walks the graph of nodes and builds valid SQL.

        The generation is organized as a pipeline that:

        Builds a dependency graph from nodes and connections.

        Performs topological sorting and detects parallel execution groups.

        Creates an execution plan with cost estimates.

        Generates SQL fragments for each node using a factory of generators (each node type has its own generator derived from BaseSQLGenerator).

        Applies PostgreSQL‑specific optimizations (CTE flattening, predicate pushdown, subquery conversion, etc.).

        Assembles the final INSERT ... SELECT statement, optionally wrapping it in a transaction and adding EXPLAIN plans.

The generator is designed to be extensible – new node types can be added by implementing a concrete generator class.

Workflow Example

    Add a source node – e.g., an Excel file.
    The system creates a foreign table over the Excel file using file_fdw (the file is first converted to CSV on the backend).

    Add transformation nodes – tFilterRow, tMap, tJoin, etc.
    Each node stores its configuration in the unified metadata model.

    Add an output node – e.g., a PostgreSQL table.
    This can be a regular table or another foreign table.

    Connect the nodes – edges define the data flow.

    Run the job – the frontend calls the SQL generator, which produces a query like:

    INSERT INTO output_table (col1, col2, ...)
SELECT t1.col1, t2.col2, ...
FROM foreign_source t1
JOIN another_foreign_source t2 ON ...
WHERE ...

 Troubleshooting

    Backend fails to start – make sure PostgreSQL is running and the credentials in .env (or the defaults) are correct.

    Port 3000 or 3001 already in use – change the ports in backend/src/server.ts and client/vite.config.ts, or kill the occupying process.

    Database drivers missing – the backend logs which drivers are missing; they are optional for many databases. For PostgreSQL only, no extra drivers are needed.

    Visual Studio does not recognise npm – ensure the Node.js workload is installed. In VS 2022, you can install it via the Visual Studio Installer → Modify → select “Node.js development”.

    🙏 Acknowledgements

    React Flow for the node-based canvas

    PostgreSQL for the powerful database engine

    Talend Open Studio for the inspiration