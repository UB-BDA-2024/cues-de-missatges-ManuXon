
# Start your FastAPI application
import fastapi
from .sensors.controller import router as sensorsRouter
from yoyo import read_migrations, get_backend
import psycopg2


app = fastapi.FastAPI(title="Senser", version="0.1.0-alpha.1")

def do_migrations():
    # Define the PostgreSQL database connection string
    db_url = "postgresql://timescale:timescale@timescale:5433/timescale"

    # Create a backend object for the database
    backend = get_backend(db_url)

    # Get a list of migration objects from the migrations directory
    migrations = read_migrations('./migrations_ts')

    # Apply the migrations to the database
    with backend.lock():
        backend.apply_migrations(backend.to_apply(migrations))

    conn = psycopg2.connect(dsn=db_url)
    conn.autocommit = True
    try:
        with conn.cursor() as cursor:
            with open('./views/views_ts.sql', 'r') as file:
                sql_commands = file.read().split(';')
                for command in sql_commands:
                    if command.strip():
                        cursor.execute(command)
    except psycopg2.Error as e:
        print("Error creating views:", e)
    finally:
            conn.close()


app.include_router(sensorsRouter)

@app.get("/")
def index():
    # Return the API name and version
    return {"name": app.title, "version": app.version}
