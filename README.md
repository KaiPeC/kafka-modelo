# kafka-modelo

Projeto foi criado apartir da necessidade de ter dados online em campanhas. 

a ideia é que o script kafka producer alimente o kafka topic e o kafka consumer alimente um banco de dados mysql, com esse mysql eu irei fazer um dashboard em Tableau e DataStudio para acompanhamento de metricas.

Na pasta onde baixou o kafka inicie o zookeeper: bin/zookeeper-server-start.sh config/zookeeper.properties

Após isso inicei o kafka: bin/kafka-server-start.sh config/server.properties

Com isso temos oq precisamos iniciado, após isso precisamos criar nosso topic kafka, que sera onde as mensagems serão publicadas. 

Crie o topic com esse comando(subistitua o "kttm", pelo nome do topic que desejar) ./bin/kafka-topics.sh --create --topic kttm --bootstrap-server localhost:9092

Após isso starte o banco mysql: sudo systemctl start mysql.service

Utilize sudo mysql para se conectar com o prompt mysql pelo usuario root. 

Agora vamos criar nosso banco de dados: CREATE DATABASE <namedatabase>;

Após isso vamos setar que usaremos o database criado: USE <namedatabase>;

Agora vamos criar nossa tabela com CREATE TABLE <nametable> (<coluna1> int NOT NULL,<coluna2> varchar(45) NULL,<coluna3> int NULL);

Com isso temos nosso database com a tabela criada. 

Agora precisa apenas adaptar os scripts publicados para o seu uso, o kafka consumer deve ficar ligado monitorando o kafka topic e o kafka producer na minha utilização sera executado de 5 em 5 min, para evitar rate limit na API. 

