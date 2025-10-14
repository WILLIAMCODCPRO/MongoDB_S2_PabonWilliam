db.createCollection("TransaccionesconRegex");

const session = db.getMongo().startSession();

const municipios = session.getDatabase("incautaciones").municipios;

const incautaciones = session.getDatabase("incautaciones").incautaciones;

const departamentos = session.getDatabase("incautaciones").departamentos;

const transacciones = session.getDatabase("incautaciones").TransaccionesconRegex;

session.startTransaction();

//Ejercicios básicos
// 1. Encuentra todos los municipios que empiezan por “San”.

const resultado1 = municipios.find(
    { "nombre_muni": { $regex: /^San/i } }
).toArray();

if (resultado1.length > 0) {
  transacciones.insertMany(resultado1)
};

//2. Lista los municipios que terminan en “ito”.

const resultado2 = municipios.find(
    { "nombre_muni": { $regex: /ito$/i } }
).toArray();

if (resultado2.length > 0) {
  transacciones.insertMany(resultado1)
};

//3. Busca los municipios cuyo nombre contenga la palabra “Valle”.

const resultado3 = municipios.find(
    { "nombre_muni": { $regex: /Valle/i } }
).toArray();

if (resultado3.length > 0) {
  transacciones.insertMany(resultado3)
};

//4. Devuelve los municipios cuyo nombre empiece por vocal.

const resultado4 = municipios.find(
    { "nombre_muni": { $regex: /^[aeiou]/i } }
).toArray();

if (resultado4.length > 0) {
  transacciones.insertMany(resultado4)
};

//5. Filtra los municipios que terminen en “al” o “el”.

const resultado5 = municipios.find(
    { "nombre_muni": { $regex: /(al|el)$/i } }
).toArray();

if (resultado5.length > 0) {
  transacciones.insertMany(resultado5)
};

// 6. Encuentra los municipios cuyo nombre contenga dos vocales seguidas.

const resultado6 = municipios.find(
    { "nombre_muni": { $regex: /[aeiou]{2}/i } }
).toArray();

if (resultado6.length > 0) {
  transacciones.insertMany(resultado6)
};

//7. Obtén todos los municipios con nombres que contengan la letra “z”.

const resultado7 = municipios.find(
    { "nombre_muni": { $regex: /z/i } }
).toArray();

if (resultado7.length > 0) {
  transacciones.insertMany(resultado7)
};

// 8. Lista los municipios que empiecen con “Santa” y tengan cualquier cosa después.

const resultado8 = municipios.find(
    { "nombre_muni": { $regex: /^Santa/i } }
).toArray();

if (resultado8.length > 0) {
  transacciones.insertMany(resultado8)
};

//9. Encuentra municipios cuyo nombre tenga exactamente 6 letras.

const resultado9 = municipios.find(
    { "nombre_muni": { $regex: /^[a-záéíóú]{6}$/i } }
).toArray();

if (resultado9.length > 0) {
  transacciones.insertMany(resultado9)
};

// 10. Filtra los municipios cuyo nombre tenga 2 palabras.

const resultado10 = municipios.find(
    { "nombre_muni": { $regex: /^[a-z]+(\s+[a-z])$/i } }
).toArray();

if (resultado10.length > 0) {
  transacciones.insertMany(resultado10)
};

//11 .Encuentra municipios cuyos nombres terminen en “ito” o “ita”.

const resultado11 = municipios.find(
    { "nombre_muni": { $regex: /(ito|ita)$/i } }
).toArray();

if (resultado11.length > 0) {
  transacciones.insertMany(resultado11)
};

//12. Lista los municipios que contengan la sílaba “gua” en cualquier posición.

const resultado12 = municipios.find(
    { "nombre_muni": { $regex: /gua$/i } }
).toArray();

if (resultado12.length > 0) {
  transacciones.insertMany(resultado12)
};

//13. Devuelve los municipios que empiecen por “Puerto” y terminen en “o”.

const resultado13 = municipios.find(
    { "nombre_muni": { $regex: /^Puerto.*o$/i } }
).toArray();

if (resultado13.length > 0) {
  transacciones.insertMany(resultado13)
};

//14.Encuentra municipios con nombres que tengan más de 10 caracteres.

const resultado14 = municipios.find(
    { "nombre_muni": { $regex: /^.{11,}/i } }
).toArray();

if (resultado14.length > 0) {
  transacciones.insertMany(resultado14)
};

//15.Busca municipios que no contengan vocales.

const resultado15 = municipios.find(
    { "nombre_muni": { $regex: /^[^aeiou]+$/i } }
).toArray();

if (resultado15.length > 0) {
  transacciones.insertMany(resultado15)
};

//16. Muestra la cantidad total incautada en municipios que empiezan con “La”.

const resultado16 = incautaciones.aggregate([

    {
       $lookup: { 
            from: "municipios",     
            localField: "cod_muni", 
            foreignField: "cod_muni",
            as: "informacionMunicipios"
        }   
    },

    {
        $unwind:"$informacionMunicipios"
    },

    {
        $match:{
            "informacionMunicipios.nombre_muni":{$regex: /^La/i}

        }
    },

    {
        $group:{
            _id: "$informacionMunicipios.nombre_muni",
            cantidadDeIncautaciones: { $sum: "$cantidad" }
        }
    },

    {
        $project:{
            _id: 0,
            municipio: "$_id",
            cantidadDeIncautaciones:1
        }
    }

]).toArray();

if (resultado16.length > 0) {
  transacciones.insertMany(resultado16)
};

// 17. Calcula el total de incautaciones en municipios cuyo nombre termine en “co”.

const resultado17 = incautaciones.aggregate([

    {
       $lookup: { 
            from: "municipios",     
            localField: "cod_muni", 
            foreignField: "cod_muni",
            as: "informacionMunicipios"
        }   
    },

    {
        $unwind:"$informacionMunicipios"
    },

    {
        $match:{
            "informacionMunicipios.nombre_muni":{$regex: /co$/i}

        }
    },

    {
        $group:{
            _id: "$informacionMunicipios.nombre_muni",
            cantidadDeIncautaciones: { $sum: "$cantidad" }
        }
    },

    {
        $project:{
            _id: 0,
            municipio: "$_id",
            cantidadDeIncautaciones:1
        }
    }

]).toArray();

if (resultado17.length > 0) {
  transacciones.insertMany(resultado17)
};

// 18. Obtén el top 5 de municipios con más incautaciones cuyo nombre contenga la letra “y”.

const resultado18 = incautaciones.aggregate([

    {
       $lookup: { 
            from: "municipios",     
            localField: "cod_muni", 
            foreignField: "cod_muni",
            as: "informacionMunicipios"
        }   
    },

    {
        $unwind:"$informacionMunicipios"
    },

    {
        $match:{
            "informacionMunicipios.nombre_muni":{$regex: /y/i}

        }
    },

    {
        $group:{
            _id: "$informacionMunicipios.nombre_muni",
            cantidadDeIncautaciones: { $sum: "$cantidad" }
        }
    },

    {
        $project:{
            _id: 0,
            municipio: "$_id",
            cantidadDeIncautaciones:1
        }
    },

    {
        $sort:{cantidadDeIncautaciones: -1}
    },

    {
        $limit: 5
    }

]).toArray();

if (resultado18.length > 0) {
  transacciones.insertMany(resultado18)
};

//19. Encuentra los municipios que empiecen por “San” y agrupa la cantidad incautada por año.

const resultado19 = incautaciones.aggregate([

    {
       $lookup: { 
            from: "municipios",     
            localField: "cod_muni", 
            foreignField: "cod_muni",
            as: "informacionMunicipios"
        }   
    },

    {
        $unwind:"$informacionMunicipios"
    },

    {
        $match:{
            "informacionMunicipios.nombre_muni":{$regex: /^San/i}

        }
    },

    {
        $group:{
            _id:{
                year:{$year: "$fecha_hecho"}
            },
            cantidadDeIncautaciones: { $sum: "$cantidad" }
        }
    },

    {
        $project:{
            _id: 0,
            year: "$_id.year",
            cantidadDeIncautaciones:1
        }
    }
]).toArray();

if (resultado19.length > 0) {
  transacciones.insertMany(resultado19)
};

//20. Lista los departamentos que tengan al menos un municipio cuyo nombre termine en “ito” o “ita”, y muestra la cantidad total incautada en ellos.

const resultado20 = incautaciones.aggregate([

    {
        $lookup: { 
            from: "departamentos",     
            localField: "cod_depto", 
            foreignField: "cod_depto",
            as: "informacionDepartamentos"
        }  
    },

    {
        $unwind: "$informacionDepartamentos"
    },

    {
       $lookup: { 
            from: "municipios",     
            localField: "cod_muni", 
            foreignField: "cod_muni",
            as: "informacionMunicipios"
        }   
    },

    {
        $unwind:"$informacionMunicipios"
    },

    {
        $match:{
            "informacionMunicipios.nombre_muni":{$regex: /(ito|ita)$/i}

        }
    },

    {
        $group:{
            _id: "$informacionDepartamentos.nombre_depto",
            cantidadDeIncautaciones: { $sum: "$cantidad" }
        }
    },

    {
        $project:{
            _id: 0,
            departamento: "$_id",
            cantidadDeIncautaciones:1
        }
    }

]).toArray();

if (resultado20.length > 0) {
  transacciones.insertMany(resultado20)
};

session.commitTransaction();

session.endSession();