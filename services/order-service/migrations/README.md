# Order Service Migrations

Apply migrations in lexical order from `versions/`.

For local SQLite testing, run:

```powershell
sqlite3 ledger_order.db ".read migrations/versions/0001_create_orders.sql"
```

For PostgreSQL, run equivalent SQL with your preferred migration runner.
