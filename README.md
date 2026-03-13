# Talend‑Inspired SQL‑Based Data Integration Platform
Visual data pipeline design meets the power of PostgreSQL Foreign Data Wrappers

---

## 📖 Overview

This project is a **React + TypeScript** application that lets you design data integration pipelines through a **graphical node‑based canvas**, inspired by the workflow of **Talend Open Studio**.  
However, instead of generating Java code, our system dynamically produces **SQL statements** – specifically `INSERT ... SELECT` queries – that move and transform data directly inside **PostgreSQL**, using **Foreign Data Wrappers (FDW)** and **foreign tables**.

The result is a lightweight, database‑centric ETL tool that leverages PostgreSQL’s mature query engine and eliminates the need for intermediate application‑layer processing.

---

## 🎯 Core Idea

- **Visual pipeline builder** – drag & drop nodes (sources, transformations, outputs) onto a canvas.
- **Node configuration** – define schemas, filters, joins, mappings, etc.
- **SQL generation** – the canvas is translated into a single (or chained) `INSERT ... SELECT` statement.
- **Foreign tables** – external data (CSV files, other databases, web services) are exposed as local PostgreSQL tables via FDW.
- **Execution** – the generated SQL is run inside PostgreSQL, moving data from source foreign tables to target tables (which may also be foreign).

This design keeps the data in the database, avoids moving large datasets to the application, and takes full advantage of PostgreSQL’s optimizations.

---

## 🏗️ Architecture
+---------------------+ +------------------------+
| React Frontend | | PostgreSQL Backend |
| (Canvas, Sidebar, | |------------------------|
| Right Panel) | --> | - FDW extensions |
+---------------------+ | - Foreign tables |
| | - Generated SQL |
v +------------------------+
+---------------------+
| SQL Generator |
| (TypeScript module) |
+---------------------+


- **Frontend**: React with TypeScript, React Flow for the canvas, Redux for state.
- **Backend**: PostgreSQL (≥ 12) with appropriate FDW extensions (e.g., `file_fdw`, `postgres_fdw`, `mysql_fdw`, `ogr_fdw`).
- **SQL Generation**: A dedicated TypeScript module that walks the graph of nodes and builds valid SQL.  
  The generation is organized as a **pipeline** that:

  1. Builds a dependency graph from nodes and connections.
  2. Performs topological sorting and detects parallel execution groups.
  3. Creates an execution plan with cost estimates.
  4. Generates SQL fragments for each node using a **factory of generators** (each node type has its own generator derived from `BaseSQLGenerator`).
  5. Applies PostgreSQL‑specific optimizations (CTE flattening, predicate pushdown, subquery conversion, etc.).
  6. Assembles the final `INSERT ... SELECT` statement, optionally wrapping it in a transaction and adding EXPLAIN plans.

The generator is designed to be extensible – new node types can be added by implementing a concrete generator class.

---

## 🔄 Workflow Example

1. **Add a source node** – e.g., an **Excel file**.  
   The system creates a foreign table over the Excel file using `file_fdw` (the file is first converted to CSV on the backend).
2. **Add transformation nodes** – `tFilterRow`, `tMap`, `tJoin`, etc.  
   Each node stores its configuration in the unified metadata model.
3. **Add an output node** – e.g., a PostgreSQL table.  
   This can be a regular table or another foreign table.
4. **Connect the nodes** – edges define the data flow.
5. **Run the job** – the frontend calls the SQL generator, which produces a query like:
   ```sql
   INSERT INTO output_table (col1, col2, ...)
   SELECT t1.col1, t2.col2, ...
   FROM foreign_source t1
   JOIN another_foreign_source t2 ON ...
   WHERE ...