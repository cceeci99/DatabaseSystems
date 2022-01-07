# Υλοποίηση Συστημάτων Βάσεων Δεδομένων
# Εργασία 2

## Προσωπικά Στοιχεία

Όνομα: Tsvetomir Ivanov Α.Μ.: 1115201900066

Όνομα: Χρήστος Γαλανόπουλος Α.Μ.: 1115201900031

## Documentation

## 1. Δομη Project και οδηγίες εκτέλεσης
...

  Προσθήκη ενιαίου αρχείου επικεφαλίδας (header file):
- ht.h: Περιέχει τις κοινές δομές (Record, HF_Info, πίνακας ανοικτών αρχείων),  για πρωτεύον και δευτερεύον ευρετήριο.

## 2. Σχεδιαστικές επιλογές και παραδοχές
Ένα hash file περιέχει:

-1 metadata block
-1 ή παραπάνω hash blocks
-2 ή παραπάνω data blocks

### a) Δομή metadata block

- αναγνωρισιτκό του hash file -> 'HashFile'
- χαρακτήρας που καθορίζει σε ποιο πεδίο κλειδί δημιουργείται το δευτερεύον -> 'c' για city, 's' για surname
- τρέχον global depth
- αριθμός hash blocks
- id για το 1ο hash block
- id για το 2ο hash block
- ...
