from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError
import certifi

# ============================================================
# OLD DATABASE (source)
# ============================================================
OLD_URI = "mongodb+srv://hashimsaqib46_db_user:CodedCloud651@cluster0.hz28edk.mongodb.net/pro_crm?retryWrites=true&w=majority&appName=Cluster0"
OLD_DB_NAME = "pro_crm"

# ============================================================
# NEW DATABASE (destination)
# ============================================================
NEW_URI = "mongodb+srv://hospitalmanagmentsystem:hos123456@cluster0.uympmdy.mongodb.net/hospital_management?retryWrites=true&w=majority"
NEW_DB_NAME = "hospital_management"

BATCH_SIZE = 500  # Insert in batches for speed

def migrate_collection(old_col, new_col):
    documents = list(old_col.find({}))
    total = len(documents)
    if total == 0:
        print(f"    [{old_col.name}] 0 documents - skip")
        return

    inserted = 0
    skipped = 0

    # Process in batches
    for i in range(0, total, BATCH_SIZE):
        batch = documents[i:i + BATCH_SIZE]
        try:
            result = new_col.insert_many(batch, ordered=False)
            inserted += len(result.inserted_ids)
        except BulkWriteError as bwe:
            # Some inserted, some were duplicates
            inserted += bwe.details.get('nInserted', 0)
            skipped += len(bwe.details.get('writeErrors', []))
        except Exception as e:
            print(f"      Batch error: {e}")

    print(f"    ✅ [{old_col.name}] {inserted} copied, {skipped} already existed (total: {total})")

def migrate():
    print("=" * 60)
    print("  MongoDB Migration: pro_crm --> hospital_management")
    print("=" * 60)

    print("\n[1] Old database se connect ho raha hun...")
    try:
        old_client = MongoClient(OLD_URI, tlsCAFile=certifi.where(), serverSelectionTimeoutMS=15000)
        old_db = old_client[OLD_DB_NAME]
        old_client.admin.command('ping')
        print(f"    ✅ Old database connected: {OLD_DB_NAME}")
    except Exception as e:
        print(f"    ❌ Old database connection fail: {e}")
        return

    print("\n[2] New database se connect ho raha hun...")
    try:
        new_client = MongoClient(NEW_URI, tlsCAFile=certifi.where(), serverSelectionTimeoutMS=15000)
        new_db = new_client[NEW_DB_NAME]
        new_client.admin.command('ping')
        print(f"    ✅ New database connected: {NEW_DB_NAME}")
    except Exception as e:
        print(f"    ❌ New database connection fail: {e}")
        return

    print("\n[3] Collections list kar raha hun...")
    collections = old_db.list_collection_names()
    if not collections:
        print("    ⚠️  Koi collection nahi mili!")
        return
    print(f"    Total collections: {len(collections)}")

    print("\n[4] Data migrate ho raha hai...\n")
    total_copied = 0

    for collection_name in collections:
        migrate_collection(old_db[collection_name], new_db[collection_name])

    print("\n" + "=" * 60)
    print("  ✅ Migration complete!")
    print("  Old database ka data safe hai - kuch delete nahi hua.")
    print("=" * 60)

    old_client.close()
    new_client.close()

if __name__ == "__main__":
    migrate()
