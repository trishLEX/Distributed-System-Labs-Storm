# Storm-Lab
Лабораторная работа по Apache Storm

Требуется разработать топологию для составления частотного словаря файлов.

Требуется разработать Spout который будет опрашивать директорию и в случае обнаружения в ней новых файлов читать ее построчно 
и генерировать tuple в исходящий поток words для каждой строки. После завершения чтения файла Spout должен выдать tuple 
в исходящий поток sync. Также после окончания файла требуется перенести файл в папку для обработанных файлов.

Требуется разработать Bolt Splitter который будет принимать строку и разбивать ее на слова.

Требуется разработать Bolt Counter который будет принимать слова из входящего потока Splitter и вести частотный словарь. 
Также по команде от входного потока sync — печатать текущий словарь на экране и обнулять его.

Запуск:
mvn compile
mvn exec:java -Dexec.mainClass="StormLab"