// ¿Cuántos municipios comienzan con "La" y cuál es la cantidad total incautada en ellos?

db.incautaciones.aggregate([
    {
        $lookup: { // Segun la siginetes indicaciones une informcaion de la colecion municipios a la colecion inactuaciones
            from: "municipios",     // De la coleccion municipios
            localField: "cod_muni", // Toma el cod_min de la coleccion incatuaciones
            foreignField: "cod_muni",// Y compara cod_min de la coleccion municipios
            as: "informacionMunicipios"// Y guarda esa informacion en un arreglo llamado informacionMunicipios
        }
    },

    {
        $unwind: "$informacionMunicipios"
    },
    
    {
        $match:{
            "informacionMunicipios.nombre_muni":{$regex: "^La", $options: "i"}
        }
    },

    {
        $group: {
            _id: null,
            totalMunicipiosConLa: { $addToSet: "$informacionMunicipios.nombre_muni" },
            cantidadTotalIncautada: { $sum: "$cantidad" }
        }
    },

    {
        $project:{
        _id: 0,
        totalMunicipios: { $size: "$totalMunicipiosConLa" },
        cantidadTotalIncautada: 1
        }
    }
]);

// Top 5 departamentos donde los municipios terminan en "al" y la cantidad incautada.

db.incautaciones.aggregate([

    {
        $lookup: { 
            from: "departamentos",     
            localField: "cod_depto", 
            foreignField: "cod_depto",
            as: "informacionDepartamentos"
        }
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
        $unwind: "$informacionMunicipios"
    },

    {
        $unwind: "$informacionDepartamentos"
    },

    {
        $match:{
            "informacionMunicipios.nombre_muni":{$regex: "al$", $options: "i"}
        }
    },

    {
        $group:{
            _id: "$informacionDepartamentos.nombre_depto",
            cantidadTotalIncautada: { $sum: "$cantidad" },
            nombreMunicipio: { $addToSet: "$informacionMunicipios.nombre_muni" }
        }
    },

    {
        $project:{
            _id: 0,
            nombreDepartamento: "$_id",
            cantidadTotalIncautada: 1,
            nombreMunicipio: 1
        }
    },

    {
        $sort: { cantidadTotalIncautada: -1 }
    },

    {
        $limit: 5 
    }
]);

// Por cada año, muestra los 3 municipios con más incautaciones, pero únicamente si su nombre contiene la letra "z".

db.incautaciones.aggregate([
    {
      $lookup: { 
            from: "municipios",     
            localField: "cod_muni", 
            foreignField: "cod_muni",
            as: "informacionMunicipios"
        }  
    },

    {
       $unwind: "$informacionMunicipios"
    },

    {
        $match:{
            "informacionMunicipios.nombre_muni":{$regex: "z", $options: "i"}
        }
    },

    {
        $group:{
            _id: {
                year: {$year: "$fecha_hecho"},
                nombreMunicipio: "$informacionMunicipios.nombre_muni"
            },
            cantidadDeIncautaciones: { $sum: 1 },
            
        }
    },

    {
        $sort: { "_id.year": 1, cantidadDeIncautaciones: -1 }
    },

    {
        $group:{
            _id: "$_id.year",
            top3Incautaciones: {
                $push:{
                    nombreMunicipio: "$_id.nombreMunicipio",
                    cantidadDeIncautaciones: "$cantidadDeIncautaciones"
                }
            }
               
            
            
        }
    },

    {
       $project:{
            _id: 0,
            year: "$_id",
            top3Incautaciones:{$slice:["$top3Incautaciones", 3]} //$slice Toma el array y escoje solo los tres primeros elementos
        } 
    },

    {
        $sort: { year: 1 }
    }
]);

// ¿Qué unidad de medida aparece en registros de municipios que empiecen por "Santa"?

db.incautaciones.aggregate([
    {
      $lookup: { 
            from: "municipios",     
            localField: "cod_muni", 
            foreignField: "cod_muni",
            as: "informacionMunicipios"
        }  
    },

    {
      $lookup: { 
            from: "unidades",     
            localField: "id_unidad", 
            foreignField: "_id",
            as: "informacionUnidades"
        }  
    },

    {
        $unwind: "$informacionUnidades"
    },

    {
        $unwind: "$informacionMunicipios",
    },

    {
        $match:{
            "informacionMunicipios.nombre_muni":{$regex: "^Santa", $options: "i"}
        }
    },

    {
        $group:{
            _id:"$informacionMunicipios.nombre_muni",
            nombreUnidadMedida:{ $first: "$informacionUnidades.nombre_unidad" }
        }
    },

    {
        $project:{
            _id:0,
            nombreMunicipio: "$_id",
            nombreUnidadMedida:1
        }
    }
]);

//¿Cuál es la cantidad promedio de incautaciones en los municipios cuyo nombre contiene "Valle"?

db.incautaciones.aggregate([
    {
        $lookup: { 
            from: "municipios",     
            localField: "cod_muni", 
            foreignField: "cod_muni",
            as: "informacionMunicipios"
        }  
    },

    {
        $unwind: "$informacionMunicipios",
    },

    {
       $match:{
            "informacionMunicipios.nombre_muni":{$regex: /SAnTa/i}
        } 
    },

    {
        $group:{
            _id:"$informacionMunicipios.nombre_muni",
            promedioCantidad: { $avg: "$cantidad" }
        }
    },

    {
         $project:{
            _id:0,
            nombreMunicipio: "$_id",
            promedioCantidad:1
        }
    }
]);

//¿Cuántos registros hay en municipios cuyo nombre contenga exactamente 7 letras?

db.incautaciones.aggregate([
     {
        $lookup: { 
            from: "municipios",     
            localField: "cod_muni", 
            foreignField: "cod_muni",
            as: "informacionMunicipios"
        }  
    },

    {
        $unwind: "$informacionMunicipios",
    },

    {
        $addFields: {
          nombreSinEspacios: { $replaceAll: { input: "$informacionMunicipios.nombre_muni", find: " ", replacement: "" } }
        }
    },

    {
        $match:{
            "nombreSinEspacios":{$regex: /^[a-zñ]{7}$/i}
        } 
    },

    {
        $group:{
            _id:"$nombreSinEspacios",
            cantidadDeIncautaciones: { $sum: 1 }
        }
    },

    {
        $project:{
            _id:0,
            nombreMunicipio: "$_id",
            cantidadDeIncautaciones:1
        }
    }
]);

//¿Cuáles son los 10 municipios con mayor cantidad incautada en 2020?

db.incautaciones.aggregate([
    {
        $group:{
            _id: {
                year: {$year: "$fecha_hecho"},
                municipio: "$cod_muni"
            },
            cantidadDeIncautaciones: { $sum: "$cantidad" }
        }
    },

    {
        $match: {
          $expr: { $eq: [  "$_id.year" , 2020 ] }
        }
    },

    {
        $lookup: { 
            from: "municipios",     
            localField: "_id.municipio", 
            foreignField: "cod_muni",
            as: "informacionMunicipios"
        }  
    },

    {
        $unwind: "$informacionMunicipios",
    },

    {
        $project:{
            _id:0,
            nombreMunicipio: "$informacionMunicipios.nombre_muni",
            fecha: "$_id.year",
            cantidadDeIncautaciones: 1
        }
    },

    {
         $sort: { cantidadDeIncautaciones: -1 } 
    },

    {
       $limit: 10 
    }


]);

// Por cada departamento, muestra el municipio con más cantidad incautada.

db.incautaciones.aggregate([
     {
        $group:{
            _id: {
                departamento: "$cod_depto",
                municipio: "$cod_muni"
            },
            cantidadDeIncautaciones: { $sum: "$cantidad" }
        }
    },

    {
      $lookup: { 
            from: "municipios",     
            localField: "_id.municipio", 
            foreignField: "cod_muni",
            as: "informacionMunicipios"
        }  
    },

    {
        $lookup: { 
            from: "departamentos",     
            localField: "_id.departamento", 
            foreignField: "cod_depto",
            as: "informacionDepartamentos"
        }  
    },

    {
        $unwind: "$informacionMunicipios"
    },

    {
        $unwind: "$informacionDepartamentos"
    },

    {
        $project:{
            _id: 0,
            departamento: "$informacionDepartamentos.nombre_depto",
            municipio:"$informacionMunicipios.nombre_muni",
            cantidadDeIncautaciones: 1
        }
    },

    {
         $sort: { departamento: 1, cantidadDeIncautaciones: -1 } 
    },

    {
        $group:{
            _id: "$departamento",
            municipio: {$first:"$municipio"},
            cantidadDeIncautaciones: {$first:"$cantidadDeIncautaciones"}
        }
    },

    {
        $project:{
            _id: 0,
            departamento: "$_id",
            municipio: 1,
            cantidadDeIncautaciones: 1
        }
    },

    {
         $sort: { departamento: 1} 
    }




]);

//Muestra la evolución mensual de incautaciones en Antioquia

db.incautaciones.aggregate([
    {
         $group:{
            _id: {
                mes: {$month: "$fecha_hecho"},
                year: {$year: "$fecha_hecho"},
                departamento: "$cod_depto"
            },
            cantidadDeIncautaciones: { $sum: 1 }
        }
    },

    {
        $lookup: { 
            from: "departamentos",     
            localField: "_id.departamento", 
            foreignField: "cod_depto",
            as: "informacionDepartamentos"
        }  
    },

    {
       $unwind: "$informacionDepartamentos"
    },

    {
        $match:{
            "informacionDepartamentos.nombre_depto":{$regex: /^Antioquia$/i}
        }
    },

    {
        $project:{
            _id:0,
            mes: "$_id.mes",
            year: "$_id.year",
            cantidadDeIncautaciones: 1

        }
    },

    {
        $sort:{year: 1, mes: 1}
    }
    
]);

//¿Cuáles son los 5 años con menor cantidad incautada en todo el país?

db.incautaciones.aggregate([
    {
         $group:{
            _id: {$year: "$fecha_hecho"},
            cantidadDeIncautaciones: { $sum: "$cantidad" }
        }
    },

    {
        $project:{
            _id: 0,
            year: "$_id",
            cantidadDeIncautaciones: 1
        }
    },

    {
        $sort:{cantidadDeIncautaciones: 1}
    },

    {
        $limit: 5
    }
]);

//Muestra la cantidad total incautada por unidad de medida, ordenada de mayor a menor.

db.incautaciones.aggregate([
    {
         $group:{
            _id: "$id_unidad",
            cantidadDeIncautaciones: { $sum: "$cantidad" }
        }
    },

    {
      $lookup: { 
            from: "unidades",     
            localField: "_id", 
            foreignField: "_id",
            as: "informacionUnidades"
        }  
    },

    {
        $unwind: "$informacionUnidades"
    },

    {
        $project:{
            _id: 0,
            nombreUnidad: "$informacionUnidades.nombre_unidad",
            cantidadDeIncautaciones:1
        }
    }
]);

// Identifica los municipios que nunca superaron 1 kilogramo de incautación en todos sus registros.

db.incautaciones.aggregate([

    {
         $group:{
            _id: "$cod_muni",
            cantidadDeIncautaciones: { $sum: "$cantidad" }
        }
    },

    {
        $match:{cantidadDeIncautaciones:{$lt:1}}
    },

    {
        $lookup: { 
            from: "municipios",     
            localField: "_id", 
            foreignField: "cod_muni",
            as: "informacionMunicipios"
        }  
    },

    {
        $unwind:"$informacionMunicipios"
    },

    {
        $project:{
            _id: 0,
            municipio: "$informacionMunicipios.nombre_muni",
            cantidadDeIncautaciones:1
        }
    }

]);

//Encuentra los municipios cuyo nombre empieza por "Puerto" y determina el total incautado en cada año.

db.incautaciones.aggregate([
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
            "informacionMunicipios.nombre_muni":{$regex: /^Puerto/i}

        }
    },

    {
        $group:{
            _id:{
                municipio:"$informacionMunicipios.nombre_muni",
                year:{$year: "$fecha_hecho"}
            },
            cantidadDeIncautaciones: { $sum: "$cantidad" }

        }
    },

    {
        $project:{
            _id: 0,
            municipio: "$_id.municipio",
            year: "$_id.year",
            cantidadDeIncautaciones:1
        }
    }
]);

//¿Cuál es el mes con más incautaciones en toda la historia para municipios que terminen en "ito" o "ita"?

db.incautaciones.aggregate([
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
            _id: {mes: {$month: "$fecha_hecho"}},
            cantidadDeIncautaciones: { $sum: 1 }
        }
    },

    {
        $project:{
            _id:0,
            mes: "$_id.mes",
            cantidadDeIncautaciones:1
        }
    },

    {
        $sort:{cantidadDeIncautaciones:-1}
    },

    {
        $limit: 1
    }

   
]);

//Construye un ranking de los 5 departamentos con mayor variación de incautaciones entre el primer y el último año registrado.

db.incautaciones.aggregate([
    {
        $group:{
            _id:{
                year:{$year: "$fecha_hecho"},
                departamento:"$cod_depto"
            },
            cantidadDeIncautaciones: { $sum: 1 }
        }
    },

    {
      $sort:{"_id.departamento": 1,"_id.year":1}
    },

    {
        $group: {
            _id: "$_id.departamento",
            incautacionesPrimerYear: { $first: "$cantidadDeIncautaciones" },
            incautacionesUltimoYear: { $last: "$cantidadDeIncautaciones" }
        }
    },

    {
        $addFields: {
            variacion: { $subtract: ["$incautacionesUltimoYear", "$incautacionesPrimerYear"] }
        }
    },

    {
        $match: { variacion: { $ne: 0 } }
    },

   {
        $lookup: { 
            from: "departamentos",     
            localField: "_id", 
            foreignField: "cod_depto",
            as: "informacionDepartamentos"
        }  
    },

    {
       $unwind: "$informacionDepartamentos"
    },

    {
        $project:{
            _id:0,
            departamento:"$informacionDepartamentos.nombre_depto",
            variacion:1
        }
    },

    {
        $sort:{variacion:-1}
    },

    {
        $limit:5
    }



]);