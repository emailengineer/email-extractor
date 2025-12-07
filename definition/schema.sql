-- Database Schema for High-Performance Email Extraction System
-- MySQL/MariaDB

CREATE DATABASE IF NOT EXISTS email_extraction CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE email_extraction;

-- Searches table: track batch extraction jobs
CREATE TABLE searches (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    batch_name VARCHAR(255),
    total_domains INT DEFAULT 0,
    status ENUM('pending', 'in_progress', 'completed', 'paused', 'cancelled') DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP NULL,
    completed_at TIMESTAMP NULL,
    INDEX idx_status (status),
    INDEX idx_created (created_at)
) ENGINE=InnoDB;

-- Domains table: track individual domains within searches
CREATE TABLE domains (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    search_id BIGINT NOT NULL,
    domain VARCHAR(255) NOT NULL,
    url VARCHAR(500) NOT NULL,
    status ENUM('pending', 'crawling', 'completed', 'failed') DEFAULT 'pending',
    worker_id VARCHAR(100) NULL,
    locked_at TIMESTAMP NULL,
    pages_crawled INT DEFAULT 0,
    emails_found INT DEFAULT 0,
    error_message TEXT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (search_id) REFERENCES searches(id) ON DELETE CASCADE,
    UNIQUE KEY unique_domain_search (search_id, domain),
    INDEX idx_search (search_id),
    INDEX idx_status (status),
    INDEX idx_worker (worker_id),
    INDEX idx_locked (locked_at)
) ENGINE=InnoDB;

-- Pages table: track crawled pages
CREATE TABLE pages (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    domain_id BIGINT NOT NULL,
    url VARCHAR(1000) NOT NULL,
    status_code INT NULL,
    content_type VARCHAR(100) NULL,
    crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error_message TEXT NULL,
    FOREIGN KEY (domain_id) REFERENCES domains(id) ON DELETE CASCADE,
    UNIQUE KEY unique_url_domain (domain_id, url(500)),
    INDEX idx_domain (domain_id),
    INDEX idx_crawled (crawled_at)
) ENGINE=InnoDB;

-- Emails table: store extracted and normalized emails
CREATE TABLE emails (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    domain_id BIGINT NOT NULL,
    page_id BIGINT NOT NULL,
    raw_email VARCHAR(255) NOT NULL,
    normalized_email VARCHAR(255) NOT NULL,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (domain_id) REFERENCES domains(id) ON DELETE CASCADE,
    FOREIGN KEY (page_id) REFERENCES pages(id) ON DELETE CASCADE,
    UNIQUE KEY unique_email_domain (domain_id, normalized_email),
    INDEX idx_domain (domain_id),
    INDEX idx_page (page_id),
    INDEX idx_normalized (normalized_email)
) ENGINE=InnoDB;

-- Statistics table: track system performance
CREATE TABLE statistics (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    search_id BIGINT NULL,
    metric_name VARCHAR(100) NOT NULL,
    metric_value BIGINT NOT NULL,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (search_id) REFERENCES searches(id) ON DELETE CASCADE,
    INDEX idx_search_metric (search_id, metric_name),
    INDEX idx_recorded (recorded_at)
) ENGINE=InnoDB;

-- Create views for easy querying
CREATE OR REPLACE VIEW search_summary AS
SELECT 
    s.id,
    s.batch_name,
    s.status,
    s.total_domains,
    COUNT(DISTINCT d.id) as domains_processed,
    SUM(d.pages_crawled) as total_pages_crawled,
    SUM(d.emails_found) as total_emails_found,
    s.created_at,
    s.started_at,
    s.completed_at,
    TIMESTAMPDIFF(SECOND, s.started_at, COALESCE(s.completed_at, NOW())) as duration_seconds
FROM searches s
LEFT JOIN domains d ON s.id = d.search_id
GROUP BY s.id;

CREATE OR REPLACE VIEW domain_details AS
SELECT 
    d.id,
    d.search_id,
    d.domain,
    d.url,
    d.status,
    d.pages_crawled,
    d.emails_found,
    d.error_message,
    d.updated_at,
    GROUP_CONCAT(DISTINCT e.normalized_email SEPARATOR ', ') as emails
FROM domains d
LEFT JOIN emails e ON d.id = e.domain_id
GROUP BY d.id;