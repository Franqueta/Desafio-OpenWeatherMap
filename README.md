#Desafio-OpenWeatherMap
Desafio Dados2Dados: Consumindo API de tempo e temperatura com PySpark e SQL
 este desafio, você irá extrair dados de uma API usando Python, armazená-los em uma base de dados SQL e executar consultas SQL para analisar e obter insights dos dados coletados. O objetivo é realizar análises e responder a algumas perguntas comuns em diferentes áreas, como finanças, saúde, esportes, entre outros.

API: Para este desafio, será utilizada a API pública do OpenWeatherMap, que fornece informações climáticas em tempo real para cidades em todo o mundo. A documentação da API está disponível em https://openweathermap.org/api.

Dados: Os dados fornecidos pela API incluem informações como temperatura, umidade, pressão e velocidade do vento. Os dados devem ser coletados por meio de solicitações HTTP para a API e armazenados em uma tabela em uma base de dados SQL.

. ..
Problemas a serem resolvidos:
Obter a temperatura atual para uma lista de cidades do seu estado e armazenar os resultados em uma tabela SQL.

Analisar a temperatura máxima e mínima para cada cidade em um período de 30 dias e exibir os resultados em uma tabela SQL.

Determinar a cidade com a maior diferença entre a temperatura máxima e mínima e exibir o resultado em uma tabela SQL.

Identificar a cidade mais quente e a cidade mais fria em um período de 30 dias e exibir os resultados em uma tabela SQL.

Calcular a média da temperatura para cada dia em um período de 30 dias e exibir os resultados em uma tabela SQL.

Identificar as cidades com as maiores e menores variações de temperatura em um período de 30 dias e exibir os resultados em uma tabela SQL.

Obter a previsão do tempo para uma lista de cidades do seu estado nos próximos 7 dias e armazenar os resultados em uma tabela SQL.

Identificar a cidade com a maior quantidade de dias chuvosos em um período de 30 dias e exibir o resultado em uma tabela SQL.

Calcular a média de umidade para cada dia em um período de 30 dias e exibir os resultados em uma tabela SQL.

Identificar as cidades com a maior e menor umidade média em um período de 30 dias e exibir os resultados em uma tabela SQL.

Observação: Para realizar este desafio, você deve ter conhecimento básico/intermediário em Python e SQL, além de ser capaz de trabalhar com APIs. Você pode utilizar quaisquer bibliotecas Python para realizar as solicitações HTTP e armazenar os dados em uma base de dados SQL, como Requests, Pandas, SQLAlchemy, entre outras. Além disso, você pode escolher qualquer banco de dados SQL, como MySQL, PostgreSQL, SQLite, entre outros.

Observação: Para o você se desafiar o ideal que você aprenda docker, pelo menos o básico para conseguir subir um container do mysql ou outro banco a seu gosto, e persista os dados nele. Caso você ache muito difícil, sugiro você salvar em formato CSV.

***PASSO A PASSO DE RESOLUÇÃO***

***Foi pego a API do tempo e em json  na aba main, foi coletado a informação de tempo atual, com isso foi colocado e formato PARQUET e inserido em um Bucket no Minio, para poder futuramente realizar um join e conseguir fazer as consultas conforme for desejado, lembrando que até a finalização do projeto vai ser trocado de parquet para delta.***

