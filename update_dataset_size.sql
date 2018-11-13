UPDATE dataset_versions
SET
  size = (
    SELECT sum(size) as size FROM files WHERE
      files.workspace = dataset_versions.workspace
      AND files.dataset_name = dataset_versions.name
      AND files.version = dataset_versions.version
      AND files.dataset_type = dataset_versions.type
  ),
  file_count = (
    SELECT count(*) as count FROM files WHERE
      files.workspace = dataset_versions.workspace
      AND files.dataset_name = dataset_versions.name
      AND files.version = dataset_versions.version
      AND files.dataset_type = dataset_versions.type
  );
