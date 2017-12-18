# Pluk
Pluk is a simple git large file system implementation done in 2 parts: git data and the data itself.

Data in git contains only links to the data chunks while data is separated by chunks and named after its SHA256 hash.
