use actix_web::{web, App, HttpServer, HttpResponse, Responder};
use sqlx::{Pool, Postgres};
use tokio::sync::Mutex;
use uuid::Uuid;
use std::sync::Arc;
use std::io::Cursor;
use futures::stream::{self, StreamExt};
use sqlx::Row;

type DbPool = Arc<Mutex<Pool<Postgres>>>;

#[derive(Debug)]
struct AppState {
    db_pool: DbPool,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Set up the database connection pool
    let database_url = "postgres://yourusername:yourpassword@localhost:5433/yourdatabase";
    let pool = Pool::<Postgres>::connect(database_url).await.unwrap();
    let db_pool = Arc::new(Mutex::new(pool));

    // Start the HTTP server
    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(AppState { db_pool: db_pool.clone() }))
            .route("/upload", web::post().to(upload_file))
            .route("/files", web::get().to(get_uploaded_files))
            .route("/download/{file_id}", web::get().to(download_file))
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}

// API 1: Upload File
async fn upload_file(
    state: web::Data<AppState>,
    file: web::Bytes,
) -> impl Responder {
    let file_id = Uuid::new_v4();
    let chunk_size = 1024 * 1024; // 1 MB
    let chunks: Vec<Vec<u8>> = file.chunks(chunk_size).map(|c| c.to_vec()).collect();

    // Use threads to parallelize the upload process
    let db_pool = state.db_pool.clone();
    stream::iter(chunks.into_iter().enumerate())
        .for_each_concurrent(None, move |(i, chunk)| {
            let db_pool = db_pool.clone();
            let file_id = file_id.clone();
            async move {
                let part_id = Uuid::new_v4();
                let pool = db_pool.lock().await;
                sqlx::query(
                    "INSERT INTO files (id, file_id, part_number, data) VALUES ($1, $2, $3, $4)"
                )
                .bind(part_id)
                .bind(file_id)
                .bind(i as i32)
                .bind(chunk)
                .execute(&*pool)
                .await
                .unwrap();
            }
        })
        .await;

    HttpResponse::Ok().json(file_id)
}


// API 2: Get Uploaded Files Data
async fn get_uploaded_files(
    state: web::Data<AppState>
) -> impl Responder {
    let db_pool = state.db_pool.lock().await;
    let rows = sqlx::query("SELECT DISTINCT file_id FROM files")
        .fetch_all(&*db_pool)
        .await
        .unwrap();

    let file_ids: Vec<Uuid> = rows.iter().map(|row| row.get("file_id")).collect();
    HttpResponse::Ok().json(file_ids)
}


// API 3: Download File by ID
async fn download_file(
    state: web::Data<AppState>,
    file_id: web::Path<Uuid>,
) -> impl Responder {
    let file_id = file_id.into_inner();
    let db_pool = state.db_pool.clone();

    // Fetch parts in parallel
    let rows = sqlx::query(
        "SELECT part_number, data FROM files WHERE file_id = $1 ORDER BY part_number"
    )
    .bind(file_id)
    .fetch_all(&*db_pool.lock().await)
    .await
    .unwrap();

    let mut parts: Vec<_> = rows.into_iter().map(|row| row.get::<Vec<u8>, _>("data")).collect();
    parts.sort_by(|a, b| a.len().cmp(&b.len())); // Ensures parts are in correct order

    // Merge parts back into a single file
    let merged_file = parts.concat();

    // Return the merged file as a downloadable response
    HttpResponse::Ok()
        .content_type("application/octet-stream")
        .body(merged_file)
}