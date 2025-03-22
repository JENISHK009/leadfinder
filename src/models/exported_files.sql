CREATE TABLE IF NOT EXISTS exported_files (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL, -- ID of the user who exported the file
    type VARCHAR(50) NOT NULL, -- Type of export (e.g., 'leads')
    export_row_count INT NOT NULL, -- Number of rows exported
    file_name VARCHAR(255) NOT NULL, -- Name of the exported file
    file_url TEXT NOT NULL, -- URL of the file in S3
    filters JSONB, -- Store filters as JSONB (optional, can be NULL)
    export_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp of the export
    CONSTRAINT fk_user
        FOREIGN KEY (user_id)
        REFERENCES users(id) -- Assuming you have a `users` table
);

CREATE INDEX idx_user_id ON exported_files (user_id);
CREATE INDEX idx_export_date ON exported_files (export_date);
CREATE INDEX idx_type ON exported_files (type);
CREATE INDEX idx_filters ON exported_files USING GIN (filters); -- Index for JSONB column