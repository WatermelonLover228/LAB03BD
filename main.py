
import pyfiglet
import subprocess

print("\n1/8 Инициализация всех текстовых файлов")

script_path = "firstStage.py"
subprocess.call(["python", script_path])

print("\n2/8 Создание таблицы с городами, странами, регионами")

script_path = "secondStage.py"
subprocess.call(["python", script_path])

print("\n3/8 Создание файлов с береговой линией и конвертация в CSV")

script_path = "thirdStage.py"
subprocess.call(["python", script_path])

print("\n4/8 Создание файлов с линией стран и конвертация в CSV")

script_path = "fourthStage.py"
subprocess.call(["python", script_path])

print("\n5/8 Конвертирование файлов dataset в .csv")

script_path = "fifthStage.py"
subprocess.call(["python", script_path])

print("\n6/8 Создание таблиц в .csv")

script_path = "sixStage.py"
subprocess.call(["python", script_path])

print("\n7/8 Экспорт таблиц из .csv в PostgreSQL")

script_path = "sevenS.py"
subprocess.call(["python", script_path])

print("\n8/8 Отрисовка данных из PostgreSQL")

script_path = "eightStage.py"
subprocess.call(["python", script_path])


