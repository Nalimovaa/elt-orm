# Python SQLAlchemy based ORM for ELT
Django-like ORM to quickly create an ELT process for loading (incremental loading) from one database to another.

# Important
Important
Before using this program, you must first create all the models in the database, in which the load will be made.

## To start: 
1. Install dependencies
2. Edit ```models.py``` for you DB tables
3. Create folders ```data``` and ```archive``` in root directory
4. Put a files with **specific datetime names** in ```data_all``` folder
5. Run ```main.py``` every day

