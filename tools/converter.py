from pathlib import Path
import asyncio
import os
from dotenv import load_dotenv
from notion_client import AsyncClient


async def main(notion_clinet: AsyncClient):

    origin_db_content = await notion_clinet.databases.query(
        database_id=os.environ["ORIGIN_DB_ID"]
    )

    new_database = await notion_clinet.databases.create(
        **{
            "parent": {
                "type": "page_id",
                "page_id": os.environ["PARENT_PAGE_ID"]
            },
            "title": [
                {
                    "type": "text",
                    "text": {
                        "content": os.environ["PROPERTY_TO_CONVERT"],
                        "link": None
                    }
                }
            ],
            "properties": {
                os.environ["PROPERTY_TO_CONVERT"]: {
                    "title": {}
                }
            }
        }
    )
    new_database_keys = {}
    for record in origin_db_content["results"]:
        new_database_keys[
            record["properties"][os.environ["PROPERTY_TO_CONVERT"]]["rich_text"][0]["plain_text"]] = ""

    for record_key in new_database_keys.keys():
        new_record = await notion_clinet.pages.create(
            **{
                "parent": {
                    "type": "database_id",
                    "database_id": new_database["id"]
                },
                "properties": {
                    os.environ["PROPERTY_TO_CONVERT"]: {
                        "title": [
                            {
                                "text": {
                                    "content": record_key
                                }
                            }
                        ]
                    }
                }
            }
        )
        new_database_keys[record_key] = new_record["id"]
    await notion_clinet.databases.update(
        database_id=os.environ["ORIGIN_DB_ID"],
        **{
            "properties": {
                f"{os.environ['PROPERTY_TO_CONVERT']}_rel": {
                    "relation": {
                        "database_id": new_database["id"],
                        "type": "dual_property",
                        "dual_property": {}
                    }
                }
            }
        }
    )
    for record in origin_db_content["results"]:

        await notion_clinet.pages.update(
            page_id=record["id"],
            **{
                "properties": {
                    f"{os.environ['PROPERTY_TO_CONVERT']}_rel": {
                        "relation": [
                            {
                                "id": new_database_keys[record["properties"][os.environ['PROPERTY_TO_CONVERT']]["rich_text"][0]["plain_text"]]}
                        ]
                    }
                }
            }
        )

if __name__ == "__main__":
    load_dotenv(Path(__file__).parent.parent / "settings.cfg")
    client = AsyncClient(auth=os.environ["NOTION_API_KEY"])
    asyncio.run(main(client))
