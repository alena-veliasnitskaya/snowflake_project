with open('data.csv', "r+", encoding="utf-8") as csv_file:
    content = csv_file.read()
    new_content = content.replace('"', '')
    with open('data2.csv', "w+", encoding="utf-8") as file:
        file.write(new_content)