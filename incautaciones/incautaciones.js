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