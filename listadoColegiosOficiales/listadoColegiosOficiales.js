// use listadoColegiosOficiales

db.createCollection("coleccionTemporal"); // Creamos una coleccion temporal

db.coleccionTemporal.updateMany({},{$rename:{"a��o":"anio"}}); // Corregimos el nombre año
