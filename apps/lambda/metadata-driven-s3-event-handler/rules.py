from abc import ABC
from abc import abstractmethod
import re
import logging
from datetime import datetime, timedelta
import boto3
import gzip
import zipfile
from io import BytesIO

logger = logging.getLogger()


class RuleBase(ABC):
    def __init__(self, name, enabled=True, **kwargs):
        self._name = name
        self.enabled = enabled
        self._args = {**kwargs}

    @property
    def name(self):
        return self._name

    @abstractmethod
    def do_execute(self, context, data):
        pass

    def on_failure(self, context, data, exception):
        logger.error(f"{self.name}: Failed due to {exception}")
        file_keys = data.get("object").get("key").split("/")
        bucket = data.get("bucket").get("name")
        file_path = "/".join(file_keys[1:-1])
        file_name = file_keys[-1]
        context.log_audit_event(bucket_name=bucket,
                                file_path=file_path,
                                file_name=file_name,
                                component_name=self.name,
                                action=f"{exception}", action_status='FAILED')
        raise exception

    def on_success(self, context, data, msg):
        logger.info(f"{self.name}: {data.get('bucket').get('name')}/{data.get('object').get('key')} {msg}")
        file_keys = data.get("object").get("key").split("/")
        bucket = data.get("bucket").get("name")
        file_path = "/".join(file_keys[1:-1])
        file_name = file_keys[-1]
        context.log_audit_event(bucket_name=bucket,
                                file_path=file_path,
                                file_name=file_name,
                                component_name=self.name,
                                action=msg, action_status='SUCCESS')

    def apply(self, context, data):
        try:
            logger.info(f"{self.name}: Executing with {data}")
            msg = self.do_execute(context, data)
            self.on_success(context, data, msg)
        except Exception as ex:
            self.on_failure(context, data, ex)


class FilenamePatternRegexRule(RuleBase):
    def __init__(self, file_name_pattern, **kwargs):
        super().__init__(name="Filename Pattern Regex Rule", **kwargs)
        self._file_name_pattern = file_name_pattern

    def do_execute(self, context, data):
        logger.info(f"{self.name}: Pattern {self._file_name_pattern}")
        file_name_pattern = re.compile(self._file_name_pattern)
        file_name = data.get('object', {}).get('key', "").split("/")[-1]
        if file_name_pattern.fullmatch(file_name):
            return f"{data.get('bucket').get('name')}/{data.get('object').get('key')} name validate successfully."
        else:
            raise Exception(f"File name {file_name} does not fit the regex pattern "
                            f"{self._file_name_pattern}")


class FilenameSizeRule(RuleBase):
    def __init__(self, min_file_size=1, max_file_size=None, **kwargs):
        super().__init__(name="File size Rule", **kwargs)
        self._min_file_size = min_file_size
        self._max_file_size = None

    def do_execute(self, context, data):
        file_size = int(data.get("object").get("size"))
        if (self._max_file_size and file_size > self._max_file_size) or file_size < self._min_file_size:
            raise Exception(f"File size for {data.get('object').get('key')} is not more than {self._min_file_size - 1}"
                            f" or is more then {self._max_file_size}." if self._max_file_size else "")
        else:
            return f"{data.get('bucket').get('name')}/{data.get('object').get('key')} size validate successfully."


class FileDuplicationCheckRule(RuleBase):
    def __init__(self, file_count=0, check_window={}, **kwargs):
        super().__init__(name="File duplication Rule", **kwargs)
        self._delta = timedelta(**check_window)
        self._file_count = file_count

    def do_execute(self, context, data):

        if self._delta.total_seconds() > 0:
            audit_events = context.get_audit_events(bucket_name=data.get("bucket").get("name"),
                                                    file_path="/".join(data.get("object").get("key").split("/")[:-1]),
                                                    file_name=data.get("object").get("key").split("/")[-1],
                                                    component_name="Context",
                                                    timestamp=(datetime.utcnow() - self._delta).strftime('%Y-%m-%d %H:%M:%S'),
                                                    action_status="SUCCESS")
        else:
            audit_events = context.get_audit_events(bucket_name=data.get("bucket").get("name"),
                                                    file_path="/".join(data.get("object").get("key").split("/")[:-1]),
                                                    file_name=data.get("object").get("key").split("/")[-1],
                                                    component_name="Context", action_status="SUCCESS")
        if len(audit_events) > self._file_count:
            raise Exception(f"Found {len(audit_events)} past successful events for {data.get('bucket').get('name')}/{data.get('object').get('key')} ")
        else:
            return f"{data.get('bucket').get('name')}/{data.get('object').get('key')} does not have any successful event in given window."


class CallGlueJob(RuleBase):
    def __init__(self, job_name, **kwargs):
        super(CallGlueJob, self).__init__(name="Glue Job Rule", **kwargs)
        self._job_name = job_name

    def do_execute(self, context, data):
        logger.info(f"{self.name}: Calling glue job: {self._job_name}")
        glue = boto3.client('glue')
        try:
            job_args = {f"--{key}": value for key,value in context.config.items() if key not in ["on_landing",
                                                                                                 "on_ingestion"] and
                        value is not None}
            for key, value in self._args.items():
                job_args[f"--{key}"] = value
            job_args["--file_path"] = f"s3://{data.get('bucket').get('name')}/{data.get('object').get('key')}"
            run_id = glue.start_job_run(JobName=self._job_name, Arguments=job_args)
            logger.info(f"{self.name}: Glue job run id is {run_id}")
        except Exception as ex:
            raise ex
        return f"Glue job {self._job_name}, successfully invoked for {data.get('bucket').get('name')}/{data.get('object').get('key')}"


class DecompressArchive(RuleBase):
    def __init__(self, target_path, target_ext="csv", **kwargs):
        super(DecompressArchive, self).__init__(name="Decompress Archive", **kwargs)
        self._target_path = target_path
        self._target_ext = target_ext

    def do_execute(self, context, data):
        s3_client = s3 = boto3.client("s3")
        s3_resource = boto3.resource('s3')

        if context.config.get("file_type", "") == "gz":
            gzip_file_name = data.get('object').get('key').split("/")[-1]
            target_file_name = "{}{}".format(gzip_file_name.rstrip(".gz"),
                                             f".{self._target_ext}" if self._target_ext else "")
            s3_client.upload_fileobj(Fileobj=gzip.GzipFile(None,
                                                           "rb",
                                                           fileobj=BytesIO(s3.get_object(
                                                               Bucket=data.get('bucket').get('name'),
                                                               Key=data.get('object').get('key'))['Body'].read())),
                                     Bucket=self._target_path.split("/")[0],
                                     Key=f"{'/'.join(self._target_path.split('/')[1:])}/{target_file_name}")
            return f"{data.get('bucket').get('name')}/{data.get('object').get('key')} is decompressed to " \
                   f"{self._target_path}/{target_file_name} successfully."

        elif context.config.get("file_type", "") == "zip":
            zip_obj = s3_resource.Object(bucket_name=data.get('bucket').get('name'), key=data.get('object').get('key'))
            buffer = BytesIO(zip_obj.get()["Body"].read())
            z = zipfile.ZipFile(buffer)
            for target_file_name in z.namelist():
                rgx_list = r'\d+\__|_?(\d+)|'
                subfolder = (re.sub(rgx_list, "", target_file_name.split('.',1)[0])).lower()
                s3_resource.meta.client.upload_fileobj(
                    z.open(target_file_name),
                    Bucket=self._target_path.split("/")[0],
                    Key= f"{'/'.join(self._target_path.split('/')[1:])}/{subfolder}/{target_file_name}")
            return f"{data.get('bucket').get('name')}/{data.get('object').get('key')} is decompressed to " \
                   f"{self._target_path}/{subfolder}/{target_file_name} successfully."
        else:
            raise Exception(f"File extention {context.config.get('file_type', '')} "
                            f"is not supported or recognised as archive type")
    
