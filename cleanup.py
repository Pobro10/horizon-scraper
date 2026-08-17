import firebase_admin
from firebase_admin import credentials, firestore

cred = credentials.Certificate("serviceAccount.json")
firebase_admin.initialize_app(cred)
db  = firestore.client()
col = db.collection("klijenti")

deleted = 0

# Stari dokumenti sa poljem 'izvor'
for izvor in ("Oglasi.me", "Patuljak.me"):
    for doc in col.where(filter=firestore.And([
        firestore.FieldFilter("izvor", "==", izvor)
    ])).stream():
        doc.reference.delete()
        deleted += 1

# Novi dokumenti sa poljem 'nap' koje sadrži izvor
for doc in col.stream():
    data = doc.to_dict()
    nap  = data.get("nap", "")
    if "Oglasi.me" in nap or "Patuljak.me" in nap:
        doc.reference.delete()
        deleted += 1

print(f"Obrisano {deleted} dokumenata.")
