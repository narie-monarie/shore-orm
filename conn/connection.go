package conn

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/narie-monarie/shore-orm/db"
	_ "github.com/sijms/go-ora/v2"
)

var (
	dbInstance  *sql.DB
	ormInstance *db.ORM
)

func InitDB(ctx context.Context, dsn string) error {
	if dbInstance != nil {
		return fmt.Errorf("database already initialized")
	}

	var err error
	dbInstance, err = sql.Open("oracle", dsn)
	if err != nil {
		return fmt.Errorf("error opening database connection: %w", err)
	}

	dbInstance.SetMaxOpenConns(25)
	dbInstance.SetMaxIdleConns(25)
	dbInstance.SetConnMaxLifetime(5 * time.Minute)

	if err = dbInstance.PingContext(ctx); err != nil {
		dbInstance.Close()
		dbInstance = nil
		return fmt.Errorf("error pinging database: %w", err)
	}

	ormInstance = db.NewORM(dbInstance)
	fmt.Printf("%sDatabase connection and ORM initialized successfully!%s\n", db.ColorGreen, db.ColorReset)
	return nil
}

func GetDB() *sql.DB {
	if dbInstance == nil {
		panic("database not initialized. Call db.InitDB() first.")
	}
	return dbInstance
}

func GetORM() *db.ORM {
	if ormInstance == nil {
		panic("ORM not initialized. Call db.InitDB() first.")
	}
	return ormInstance
}

func CloseDB() error {
	if dbInstance != nil {
		fmt.Println("Closing database connection...")
		err := dbInstance.Close()
		dbInstance = nil
		ormInstance = nil
		return err
	}
	return nil
}
