UPDATE dataset_versions
SET
size = (
  SELECT sum(size) as size from files where files.workspace = dataset_versions.workspace
  AND files.dataset_name = dataset_versions.name
  AND files.version = dataset_versions.version
);
