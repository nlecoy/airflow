# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import json

from tempfile import NamedTemporaryFile
from typing import Optional, Union

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.utils.decorators import apply_defaults


class MongoToGCSOperator(BaseOperator):
    template_fields = ("mongo", "bucket", "filename")
    ui_color = "#3fa037"

    @apply_defaults
    def __init__(
        self,
        *,
        bucket: str,
        filename: str,
        mongo_conn_id: str,
        mongo_collection: str,
        mongo_query: Union[list, dict],
        gcp_conn_id: str,
        mongo_db: Optional[str] = None,
        approx_max_file_size_bytes: int = 1900000000,
        null_marker: Optional[str] = None,
        gzip: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.bucket = bucket
        self.filename = filename
        self.mongo_conn_id = mongo_conn_id
        self.mongo_collection = mongo_collection
        self.mongo_db = mongo_db
        self.mongo_query = mongo_query
        self.gcp_conn_id = gcp_conn_id
        self.approx_max_file_size_bytes = approx_max_file_size_bytes
        self.null_marker = null_marker
        self.gzip = gzip
        self.is_pipeline = isinstance(self.mongo_query, list)

    def execute(self, context):
        self.log.info("Executing query")
        mongo_hook = MongoHook(self.mongo_conn_id)
        if self.is_pipeline:
            cursor = mongo_hook.aggregate(
                mongo_collection=self.mongo_collection,
                aggregate_query=self.mongo_query,
                mongo_db=self.mongo_db,
            )
        else:
            cursor = mongo_hook.find(
                mongo_collection=self.mongo_collection, query=self.mongo_query, mongo_db=self.mongo_db,
            )

        self.log.info("Writing local data files")
        files_to_upload = self._write_local_data_files(cursor)
        for tmp_file in files_to_upload:
            tmp_file["file_handle"].flush()

        self.log.info("Uploading {} files to GCS.".format(len(files_to_upload)))
        self._upload_to_gcs(files_to_upload)

        self.log.info("Removing local files")
        for tmp_file in files_to_upload:
            tmp_file["file_handle"].close()

    def _write_local_data_files(self, cursor):
        """
        Takes a PyMongo cursor and writes documents to a local file.

        :return: A dictionary where keys are filenames to be used as object
            names in GCS, and values are file handles to local files that
            contain the data for the GCS objects.
        """
        file_no = 0
        tmp_file_handle = NamedTemporaryFile(delete=True)
        files_to_upload = [{"file_name": self.filename.format(file_no), "file_handle": tmp_file_handle}]

        self.log.info("Current file count: {}".format(len(files_to_upload)))
        for doc in cursor:
            # Write query result in JSON Lines format (i.e: append new line between docs)
            # for GCS to BigQuery imports compatibility.
            # Learn more about GCS to BigQuery import: https://cloud.google.com/bigquery/docs/loading-data
            # Learn more about JSON Lines format: https://jsonlines.org/
            tmp_file_handle.write(json.dumps(doc, sort_keys=True, ensure_ascii=False).encode("utf-8"))
            tmp_file_handle.write(b'\n')

            # Stop if the file exceeds the file size limit.
            if tmp_file_handle.tell() >= self.approx_max_file_size_bytes:
                file_no = file_no + 1
                tmp_file_handle = NamedTemporaryFile(delete=True)
                files_to_upload.append(
                    {"file_name": self.filename.format(file_no), "file_handle": tmp_file_handle}
                )
                self.log.info("Current file count: {}".format(len(files_to_upload)))

        return files_to_upload

    def _upload_to_gcs(self, files_to_upload):
        """
        Upload all of the file splits (and optionally the schema .json file)
        to Google Cloud Storage.
        """
        gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
        for tmp_file in files_to_upload:
            gcs_hook.upload(
                self.bucket,
                tmp_file.get("file_name"),
                tmp_file.get("file_handle").name,
                mime_type="application/json",
                gzip=self.gzip,
            )
