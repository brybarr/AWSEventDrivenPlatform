from setuptools import setup, find_packages
import __metadata__

setup(
    name=__metadata__.__title__,
    version=__metadata__.__version__,
    description=__metadata__.__description__,
    packages=find_packages(),
    entry_points={'dp.rules': ['s3.file_name_regex = rules:FilenamePatternRegexRule,'
                                         's3.file_size = rules:FilenameSizeRule',
                                         's3.duplication_check = rules:FileDuplicationCheckRule']}
)
