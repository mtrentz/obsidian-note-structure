import boto3
import tarfile
import os
import shutil
from datetime import datetime
import pathlib
import json


def decompress(archive_name: str, extract_dir: str) -> None:
    with tarfile.open(archive_name, "r:gz") as tar:
        tar.extractall(extract_dir)
    return


def get_note_creation_date(note_file: str) -> str:
    """
    Rule here is a bit more complicated than just getting the ctime.
    1. Open the note, if it has metadata, which is defined by a '---' at the top
    of the file, then use the date from the metadata.
    2. If it doesn't have metadata then I check if the date is Periodic,
    if it is, I will get the date from the name of the file, having logic
    for both daily, weekly and monthly note.
    3. Finally, I get the oldest from the ctime, mtime, atime and birth,
    watching out to not get dates in 1970.
    """
    with open(note_file, "r") as f:
        content = f.read()

    if content.startswith("---"):
        # Get the date from the metadata
        lines = content.split("\n")
        for line in lines:
            if line.startswith("date:"):
                return line.split(" ")[1]

    # Check if /Periodic/ is in the path
    if "/Periodic/" in note_file:
        # Get the basename of the file
        fname = os.path.basename(note_file)

        # If /Daily Notes/ is in the path then
        # the file is like this: 2021-07-23 (Friday).md
        # so just split by space and get the first part
        if "/Daily Notes/" in note_file:
            return fname.split(" ")[0]

        # If /Weekly Notes/ is in the path then
        # its like 2023-W29.md. So get this part
        # transform this into the first day of the week
        # and return it
        if "/Weekly Notes/" in note_file:
            year, week = fname.removesuffix(".md").split("-W")
            week = int(week)
            d = datetime.strptime(f"{year}-W{week}-1", "%Y-W%W-%w")
            return d.strftime("%Y-%m-%d")

        # If /Monthly Notes/ is in the path then
        # its 2023-07.md. So just add -01 to the end
        # and return it
        if "/Monthly Notes/" in note_file:
            return fname.removesuffix(".md") + "-01"

        # If /Yearly Notes/ is in the path then
        # its 2023.md. So just add -01-01 to the end
        # and return it
        if "/Yearly Notes/" in note_file:
            return fname.removesuffix(".md") + "-01-01"

    # Finally then get the stats of the file
    # and return the oldest date
    stats = os.stat(note_file)

    # birthtime can fail
    try:
        birthtime = (datetime.fromtimestamp(stats.st_birthtime),)
    except Exception as e:
        birthtime = datetime.fromtimestamp(1)

    dates = [
        datetime.fromtimestamp(stats.st_ctime),
        datetime.fromtimestamp(stats.st_mtime),
        datetime.fromtimestamp(stats.st_atime),
        birthtime,
    ]
    # Remove all older than 1990 to make sure
    dates = [d for d in dates if d.year > 1990]
    return min(dates).strftime("%Y-%m-%d")


def note_to_json(note_file: str) -> dict:
    stats = os.stat(note_file)
    # Path is everything except the final file and the vault
    path = "/".join(note_file.split("/")[1:-1])
    # Folder is the last folder in the path
    folder = path.split("/")[-1]

    with open(note_file, "r") as f:
        content = f.read()

    data = {
        "title": os.path.basename(note_file).removesuffix(".md"),
        "created_date": get_note_creation_date(note_file),
        "modified_date": datetime.fromtimestamp(stats.st_mtime).strftime("%Y-%m-%d"),
        "modified_time": datetime.fromtimestamp(stats.st_mtime).strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
        "path": path,
        "folder": folder,
        "content": content,
    }

    return data


def upload_directory_to_s3(local_dir, bucket_name):
    s3 = boto3.client("s3")

    for root, dirs, files in os.walk(local_dir):
        for file in files:
            local_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_path, local_dir)
            s3_path = os.path.join(bucket_name, relative_path).replace("\\", "/")

            # Remove the leading directory name (vault) from the S3 path
            # to upload directly to the root of the bucket
            s3_path = "/".join(s3_path.split("/")[1:])

            s3.upload_file(local_path, bucket_name, s3_path)


def lambda_handler(event, context):
    source_bucket = "source"
    destination_bucket = "dest"

    s3_client = boto3.client("s3")

    # List all objects in the source bucket
    response = s3_client.list_objects_v2(Bucket=source_bucket)

    # Find the latest zip file (assuming zip files have a .zip extension)
    latest_zip_key = max(response["Contents"], key=lambda obj: obj["LastModified"])[
        "Key"
    ]

    # Download the latest zip file to the /var/task directory
    tmp_file = os.path.join("/tmp", latest_zip_key)
    s3_client.download_file(source_bucket, latest_zip_key, tmp_file)

    vault = "/tmp/vault"

    decompress(tmp_file, vault)

    # Walk through the lambda_tmp_dir and remove all files that are not .md
    # Also remove directories that start with . or _
    for root, dirs, files in os.walk(vault):
        for dir in dirs:
            if dir.startswith(".") or dir.startswith("_"):
                shutil.rmtree(os.path.join(root, dir))
        for file in files:
            if not file.endswith(".md"):
                os.remove(os.path.join(root, file))

    # Walk again through lambda_tmp_dir and convert all .md files to JSON
    # delete the .md file
    for root, dirs, files in os.walk(vault):
        for file in files:
            if not file.endswith(".md"):
                continue
            note = os.path.join(root, file)
            data = note_to_json(note)
            json_file = note.removesuffix(".md") + ".json"
            # Place it where the .md file was
            with open(json_file, "w") as f:
                json.dump(data, f)

            os.remove(note)

    # Get all directories directly under vault with the /vault in front
    first_level = [os.path.join(vault, d) for d in os.listdir(vault)]

    # Go over each folder in the first level
    # get all their files and move them to the first level.
    # Afterward deletes every file under each first level.
    for folder in first_level:
        # Get, recursively, all files under this folder
        files = list(pathlib.Path(folder).rglob("*.*"))
        # Move each file to the first level if its not already there
        for file in files:
            if file.parent != pathlib.Path(vault):
                try:
                    shutil.move(file, folder)
                except Exception as e:
                    pass

        # List all directories under this folder
        dirs = [d for d in os.listdir(folder) if os.path.isdir(os.path.join(folder, d))]
        # Delete each directory
        for dir in dirs:
            shutil.rmtree(os.path.join(folder, dir))

    # Delete everything from the destination bucket
    s3_resource = boto3.resource("s3")
    s3_resource.Bucket(destination_bucket).objects.all().delete()

    # # Upload the modified contents of the lambda_tmp_dir to the destination bucket
    upload_directory_to_s3(vault, destination_bucket)

    return {
        "statusCode": 200,
        "body": "Latest zip file processed and uploaded successfully!",
    }
