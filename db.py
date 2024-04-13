import csv
import MySQLdb
import os

def connect_database():
    try:
        db = MySQLdb.connect("localhost", "root", "", "localidades")
        print("Conexión a la base de datos exitosa")
        return db
    except MySQLdb.Error as e:
        print("Error al conectar a la base de datos:", e)
        return None

def create_table(cursor):
    try:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS localidades (
            provincia VARCHAR(100) NOT NULL,
            id INT NOT NULL,
            localidad VARCHAR(100) NOT NULL,
            cp INT NOT NULL,
            id_prov_mstr INT NOT NULL
        )
        """
        cursor.execute(create_table_query)
        print("Tabla creada con éxito")
    except MySQLdb.Error as error:
        print("Error:", error)

def insert_data(cursor):
    try:
        with open("localidades.csv", newline="", encoding="utf-8") as csv_file:
            reader = csv.reader(csv_file)
            next(reader)
            for row in reader:
                cursor.execute(
                    "INSERT INTO localidades (provincia, id, localidad, cp, id_prov_mstr) VALUES (%s, %s, %s, %s, %s)",
                    row
                )
        print("Datos insertados con éxito")
    except MySQLdb.Error as error:
        print("Error:", error)

def create_csv_files(cursor):
    try:
        if not os.path.exists("csv"):
            os.makedirs("csv")
        
        cursor.execute("SELECT DISTINCT provincia FROM localidades")
        provincias = cursor.fetchall()
        for provincia in provincias:
            cursor.execute("SELECT * FROM localidades WHERE provincia = %s", (provincia[0],))
            localidades = cursor.fetchall()
            with open(f"csv/Localidades de {provincia[0]}.csv", "w", newline="", encoding="utf-8") as csv_file:
                writer = csv.writer(csv_file)
                writer.writerows(localidades)
        print("Archivos CSV creados con éxito")
    except MySQLdb.Error as error:
        print("Error:", error)

def main():
    db = connect_database()
    if not db:
        return

    cursor = db.cursor()
    
    create_table(cursor)
    insert_data(cursor)
    create_csv_files(cursor)

    db.close()

if __name__ == "__main__":
    main()