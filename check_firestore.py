import firebase_admin
from firebase_admin import credentials, firestore

cred = credentials.Certificate("serviceAccount.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

docs = list(db.collection("klijenti").limit(3).stream())
print(f"Ukupno dohvaceno: {len(docs)} dokumenata\n")

for i, doc in enumerate(docs, 1):
    print("-"*50)
    print(f"Dokument {i}  (ID: {doc.id})")
    print("-"*50)
    for field, value in sorted(doc.to_dict().items()):
        print(f"  {field:<12} : {value}")
    print()
