-- ============================================================================
-- AWS Athena Queries for Extracting ATS Companies from Common Crawl
-- ============================================================================
-- Common Crawl provides public access to its Parquet Columnar Index on AWS S3.
-- You can run these queries directly in the AWS Athena console against the
-- "ccindex" database with zero setup.
--
-- Table Location: s3://commoncrawl/cc-index/table/cc-main/warc/
-- Partitioned by: crawl (varchar), subset (varchar)
-- ============================================================================

-- 1. Create table (if not already present in your Athena catalog)
CREATE EXTERNAL TABLE IF NOT EXISTS ccindex (
  url_surtkey                   VARCHAR,
  url                           VARCHAR,
  url_host_name                 VARCHAR,
  url_host_tld                  VARCHAR,
  url_host_2nd_last_part        VARCHAR,
  url_host_3rd_last_part        VARCHAR,
  url_host_4th_last_part        VARCHAR,
  url_host_5th_last_part        VARCHAR,
  url_host_registry_suffix      VARCHAR,
  url_host_registered_domain    VARCHAR,
  url_host_private_suffix       VARCHAR,
  url_host_private_domain       VARCHAR,
  url_host_name_reversed        VARCHAR,
  url_protocol                  VARCHAR,
  url_port                      INT,
  url_path                      VARCHAR,
  url_query                     VARCHAR,
  fetch_time                    TIMESTAMP,
  fetch_status                  SMALLINT,
  content_digest                VARCHAR,
  content_mime_type             VARCHAR,
  content_mime_detected         VARCHAR,
  content_charset               VARCHAR,
  content_languages             VARCHAR,
  warc_filename                 VARCHAR,
  warc_record_offset            INT,
  warc_record_length            INT,
  warc_segment                  VARCHAR
)
PARTITIONED BY (
  crawl                         VARCHAR,
  subset                        VARCHAR
)
STORED AS PARQUET
LOCATION 's3://commoncrawl/cc-index/table/cc-main/warc/';

-- Discover partitions:
MSCK REPAIR TABLE ccindex;


-- ============================================================================
-- 2. Extract All Companies Across Major ATS Platforms (Single Monthly Crawl)
-- Run time: ~15-30 seconds across 3+ billion crawled web pages.
-- ============================================================================

WITH ats_base AS (
  SELECT
    CASE
      WHEN url_host_registered_domain = 'greenhouse.io' THEN 'Greenhouse'
      WHEN url_host_registered_domain = 'lever.co' THEN 'Lever'
      WHEN url_host_registered_domain = 'ashbyhq.com' THEN 'Ashby'
      WHEN url_host_registered_domain = 'workable.com' THEN 'Workable'
      WHEN url_host_registered_domain = 'smartrecruiters.com' THEN 'SmartRecruiters'
      WHEN url_host_registered_domain = 'myworkdayjobs.com' THEN 'Workday'
      WHEN url_host_registered_domain = 'bamboohr.com' THEN 'BambooHR'
      WHEN url_host_registered_domain = 'recruitee.com' THEN 'Recruitee'
      WHEN url_host_registered_domain = 'applytojob.com' THEN 'JazzHR'
      WHEN url_host_registered_domain = 'jobvite.com' THEN 'Jobvite'
      WHEN url_host_registered_domain = 'rippling.com' THEN 'Rippling'
      WHEN url_host_registered_domain = 'breezy.hr' THEN 'Breezy HR'
      WHEN url_host_registered_domain = 'pinpointhq.com' THEN 'Pinpoint'
      ELSE 'Other'
    END AS ats_platform,

    -- Extract company slug depending on path vs subdomain structure
    LOWER(
      CASE
        WHEN url_host_registered_domain = 'greenhouse.io'
          THEN regexp_extract(url, '^https?://(?:boards|job-boards)\.greenhouse\.io/([^/?#]+)', 1)
        WHEN url_host_registered_domain = 'lever.co'
          THEN regexp_extract(url, '^https?://jobs\.lever\.co/([^/?#]+)', 1)
        WHEN url_host_registered_domain = 'ashbyhq.com'
          THEN regexp_extract(url, '^https?://jobs\.ashbyhq\.com/([^/?#]+)', 1)
        WHEN url_host_registered_domain = 'workable.com'
          THEN regexp_extract(url, '^https?://apply\.workable\.com/([^/?#]+)', 1)
        WHEN url_host_registered_domain = 'smartrecruiters.com'
          THEN regexp_extract(url, '^https?://jobs\.smartrecruiters\.com/([^/?#]+)', 1)
        WHEN url_host_registered_domain = 'jobvite.com'
          THEN regexp_extract(url, '^https?://jobs\.jobvite\.com/([^/?#]+)', 1)
        WHEN url_host_registered_domain = 'rippling.com'
          THEN regexp_extract(url, '^https?://ats\.rippling\.com/([^/?#]+)', 1)
        WHEN url_host_registered_domain = 'myworkdayjobs.com'
          THEN regexp_extract(url_host_name, '^([^.]+)\.(?:wd\d+\.)?myworkdayjobs\.com$', 1)
        WHEN url_host_registered_domain = 'bamboohr.com'
          THEN regexp_extract(url_host_name, '^([^.]+)\.bamboohr\.com$', 1)
        WHEN url_host_registered_domain = 'recruitee.com'
          THEN regexp_extract(url_host_name, '^([^.]+)\.recruitee\.com$', 1)
        WHEN url_host_registered_domain = 'applytojob.com'
          THEN regexp_extract(url_host_name, '^([^.]+)\.applytojob\.com$', 1)
        WHEN url_host_registered_domain = 'breezy.hr'
          THEN regexp_extract(url_host_name, '^([^.]+)\.breezy\.hr$', 1)
        WHEN url_host_registered_domain = 'pinpointhq.com'
          THEN regexp_extract(url_host_name, '^([^.]+)\.pinpointhq\.com$', 1)
        ELSE NULL
      END
    ) AS company_slug,

    url,
    warc_filename,
    warc_record_offset,
    warc_record_length
  FROM ccindex
  WHERE subset = 'warc'
    AND crawl = 'CC-MAIN-2024-22' -- Replace with latest crawl (e.g. CC-MAIN-2026-43)
    AND url_host_tld IN ('io', 'co', 'com', 'hr')
    AND url_host_registered_domain IN (
      'greenhouse.io', 'lever.co', 'ashbyhq.com', 'workable.com',
      'smartrecruiters.com', 'myworkdayjobs.com', 'bamboohr.com',
      'recruitee.com', 'applytojob.com', 'jobvite.com', 'rippling.com',
      'breezy.hr', 'pinpointhq.com'
    )
    AND fetch_status = 200
)
SELECT
  ats_platform,
  company_slug,
  COUNT(*) AS total_crawled_urls,
  MIN(url) AS sample_job_url,
  MIN(warc_filename) AS sample_warc_filename,
  MIN(warc_record_offset) AS sample_warc_offset,
  MIN(warc_record_length) AS sample_warc_length
FROM ats_base
WHERE company_slug IS NOT NULL
  AND length(company_slug) > 1
  AND company_slug NOT IN ('www', 'app', 'static', 'assets', 'api', 'help', 'search', 'apply')
GROUP BY ats_platform, company_slug
ORDER BY total_crawled_urls DESC;

