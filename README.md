# logic-lock
[MySQL logic locks](https://dev.mysql.com/doc/refman/5.7/en/locking-functions.html#function_get-lock) implemented over sea-orm

## Locking

`Lock::build` takes a key, any owned connection, so it can be a `sea-orm::DatabaseConnection`, a `sea-orm::DatabaseTransaction` or another `Lock` himself, and an optional timeout in seconds, defaulting to 1 second.<br />
In case of error, the owned connection is returned alongside the database error, if any, this way the connection isn't automatically dropped.<br />
`Lock` himself acts as a connection, so you can use it everywhere you would have used the original connection.

```rust
let lock = logic_lock::Lock::build("my_lock_key", conn, None).await.unwrap();
```

## Unlocking

Since MySQL logic locks lives in the session, not in a transaction like table locks, a not-dropped lock will live as long as the connection, and if a connection is part of a connection pool, this can be a really long time.<br />
To release a `Lock` simply use the `Lock::release` method.<br />
On success it will return the original connection, on error it will return the `Lock` himself alongside with the database error, if any.

```rust
let conn = lock.release().await.unwrap();
```

## Drop

Dropping a locked `Lock` will log an error.<br />
It has been chosen to not panic to avoid cases of panic-while-panicking.
