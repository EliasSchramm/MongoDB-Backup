from datetime import datetime
import pymongo
import sys
from pathlib import Path
import os
import json

BLACKLISTED_DATABASES = ["config"]


def main():
    if len(sys.argv) == 1:
        print("python3 backup.py <mongo_uri>")
        exit(-1)

    mongo_uri = sys.argv[1]
    start_time = int(datetime.now().timestamp())
    client = pymongo.MongoClient(mongo_uri)

    total_size = 0
    for database_name in client.list_database_names():
        if database_name not in BLACKLISTED_DATABASES:
            print("Downloading {}...".format(database_name))

            database_start = int(datetime.now().timestamp())
            database_size = 0

            database = client[database_name]
            for collection_name in database.list_collection_names():
                collection_start = int(datetime.now().timestamp())

                collection = database[collection_name]
                count_documents = collection.count_documents({})
                print(
                    "     Downloading {} documents from {}...".format(
                        count_documents, collection_name
                    )
                )

                path = Path(
                    "./backups/{timestamp}/{database}".format(
                        timestamp=start_time,
                        database=database_name,
                        collection=collection_name,
                    )
                )

                path.mkdir(parents=True, exist_ok=True)

                file_name = "{}/{}.json".format(str(path), collection_name)

                with open(
                    file_name,
                    "w",
                    encoding="UTF-8",
                ) as file:
                    file.write("[")

                    content = collection.find(
                        {}, projection={"_id": False}, allow_disk_use=True
                    )

                    index = 1
                    for entry in content:
                        file.write(json.dumps(entry, default=str))

                        if not index == count_documents:
                            file.write(",")

                        index += 1

                    file.write("]")

                collection_size = os.path.getsize(file_name)
                database_size += collection_size

                print("     ...Done")
                print(
                    "     Size: {}MB ({}b)".format(
                        int(collection_size / 1000000), collection_size
                    )
                )
                print(
                    "     Took {} seconds".format(
                        int(datetime.now().timestamp()) - collection_start
                    )
                )

            total_size += database_size

            print(
                "Took {} seconds".format(
                    int(datetime.now().timestamp()) - database_start
                )
            )
            print(
                "Size: {}MB ({}b)".format(int(database_size / 1000000), database_size)
            )
            print("...Done")

    print(
        "Backup took a total of {} minutes".format(
            int((datetime.now().timestamp() - start_time) / 60)
        )
    )
    print("Total Size: {}MB ({}b)".format(int(total_size / 1000000), total_size))
    client.close()


if __name__ == "__main__":
    main()
