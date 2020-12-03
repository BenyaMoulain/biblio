# Laboratorio 2 - Sistemas Distribuidos

Equipo : FalopApps
Nombre : Benjamín Molina
Rol : 201573005-9

Nombre : Tomás Escalona
Rol : 201573031-8

Ejecución - Iniciar un Datanode

Ingresar en Terminal:

cd biblio
make
./Datanode --hostname <IP_DE_ESTE_NODO> --namenode_ip <IP_DEL_NAMENODE>

Ejecución - Iniciar un Cliente

cd biblio
make
./Uploader --file <RUTA_DEL_ARCHIVO>

Ejecución - Iniciar un Namenode:

cd biblio
make
./Namenode --hostname <IP_DE_ESTE_NODO>

Comandos adicionales:

Los ejecutables cuentan con más argumentos adicionales, para mayor información usar --help, por ej:

cd biblio
make
./Datanode --help
