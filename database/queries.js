/**
 * CONSULTAS DE NEGOCIO - APPLE MUSIC
 * Ejecutar con: node database/queries.js
 */

const { MongoClient } = require('mongodb');

const URI = 'mongodb://localhost:27017';
const DB_NAME = 'apple_music_db';

async function runQueries() {
    const client = new MongoClient(URI);

    try {
        await client.connect();
        const db = client.db(DB_NAME);
        console.log("Conectado a la BD para realizar consultas...\n");

        // ==========================================
        // 1. REPORTE DE REGALÍAS
        // ¿Cuánto tiempo total se ha reproducido cada artista en el último mes?
        // ==========================================
        console.log("--- CONSULTA 1: Reporte de Regalías (Top 5) ---");
        
        // Fecha de hace 30 días
        const lastMonth = new Date();
        lastMonth.setDate(lastMonth.getDate() - 30);

        const royaltiesQuery = await db.collection('streams').aggregate([
            // 1. Filtrar streams de los últimos 30 días
            { $match: { date: { $gte: lastMonth } } },
            // 2. Agrupar por Artista y sumar segundos
            { 
                $group: { 
                    _id: "$artist_id", 
                    totalSeconds: { $sum: "$seconds_played" } 
                } 
            },
            // 3. Traer datos del artista (Nombre)
            {
                $lookup: {
                    from: "artists",
                    localField: "_id",
                    foreignField: "_id",
                    as: "artist_info"
                }
            },
            // 4. Aplanar el array de artist_info
            { $unwind: "$artist_info" },
            // 5. Proyección final bonita
            {
                $project: {
                    artist: "$artist_info.name",
                    total_play_time_seconds: "$totalSeconds"
                }
            },
            { $sort: { total_play_time_seconds: -1 } }, // Ordenar descendente
            { $limit: 5 } // Solo mostramos 5 para no llenar la consola
        ]).toArray();
        console.table(royaltiesQuery);


        // ==========================================
        // 2. EL TOP 10 REGIONAL
        // Canciones más escuchadas en 'Guatemala' (GT) en los últimos 7 días
        // ==========================================
        console.log("\n--- CONSULTA 2: Top 10 Canciones en Guatemala (Últimos 7 días) ---");
        
        const lastWeek = new Date();
        lastWeek.setDate(lastWeek.getDate() - 7);

        const topRegionalQuery = await db.collection('streams').aggregate([
            // 1. Filtrar por fecha reciente
            { $match: { date: { $gte: lastWeek } } },
            // 2. Unir con Usuarios para saber su país
            {
                $lookup: {
                    from: "users",
                    localField: "user_id",
                    foreignField: "_id",
                    as: "user_info"
                }
            },
            { $unwind: "$user_info" },
            // 3. Filtrar solo usuarios de GT
            { $match: { "user_info.country": "GT" } },
            // 4. Agrupar por canción y contar reproducciones
            {
                $group: {
                    _id: "$song_id",
                    streams: { $count: {} }
                }
            },
            // 5. Traer datos de la canción
            {
                $lookup: {
                    from: "songs",
                    localField: "_id",
                    foreignField: "_id",
                    as: "song_info"
                }
            },
            { $unwind: "$song_info" },
            // 6. Formato Final
            {
                $project: {
                    song: "$song_info.title",
                    artist: "$song_info.artist_name",
                    streams: 1
                }
            },
            { $sort: { streams: -1 } },
            { $limit: 10 }
        ]).toArray();
        console.table(topRegionalQuery);


        // ==========================================
        // 3. DETECCIÓN DE USUARIOS ZOMBIS
        // Usuarios Premium que NO han escuchado nada en 30 días
        // ==========================================
        console.log("\n--- CONSULTA 3: Usuarios Zombis (Premium Inactivos) ---");

        const zombiesQuery = await db.collection('users').aggregate([
            // 1. Solo usuarios Premium
            { $match: { subscription: "Premium" } },
            // 2. Buscar sus streams recientes (Left Join)
            {
                $lookup: {
                    from: "streams",
                    let: { userId: "$_id" },
                    pipeline: [
                        { 
                            $match: { 
                                $expr: { 
                                    $and: [
                                        { $eq: ["$user_id", "$$userId"] },
                                        { $gte: ["$date", lastMonth] } // Variable lastMonth definida arriba
                                    ]
                                } 
                            } 
                        }
                    ],
                    as: "recent_streams"
                }
            },
            // 3. Filtrar los que tengan el array de streams VACÍO
            { $match: { recent_streams: { $size: 0 } } },
            // 4. Mostrar datos
            {
                $project: {
                    username: 1,
                    email: 1,
                    subscription: 1
                }
            },
            { $limit: 5 }
        ]).toArray();
        console.table(zombiesQuery);


        // ==========================================
        // 4. DEMOGRAFÍA POR GÉNERO
        // Distribución de edades de usuarios que escuchan 'Reggaeton'
        // ==========================================
        console.log("\n--- CONSULTA 4: Demografía oyentes de Reggaeton ---");

        const demoQuery = await db.collection('streams').aggregate([
            // 1. Traer info de la canción para filtrar por género
            {
                $lookup: {
                    from: "songs",
                    localField: "song_id",
                    foreignField: "_id",
                    as: "song"
                }
            },
            { $unwind: "$song" },
            { $match: { "song.genre": "Reggaeton" } },
            // 2. Traer info del usuario para saber su edad
            {
                $lookup: {
                    from: "users",
                    localField: "user_id",
                    foreignField: "_id",
                    as: "user"
                }
            },
            { $unwind: "$user" },
            // 3. Calcular edad (Proyección auxiliar)
            {
                $project: {
                    age: {
                        $dateDiff: {
                            startDate: "$user.birth_date",
                            endDate: new Date(),
                            unit: "year"
                        }
                    }
                }
            },
            // 4. Crear Buckets (Rangos de edad)
            {
                $bucket: {
                    groupBy: "$age",
                    boundaries: [0, 18, 25, 35, 50, 100],
                    default: "Other",
                    output: {
                        count: { $sum: 1 }
                    }
                }
            }
        ]).toArray();
        console.table(demoQuery);


        // ==========================================
        // 5. HEAVY USERS (GAMIFICATION)
        // Top 5 usuarios que han escuchado más canciones DISTINTAS de 'Bad Bunny'
        // ==========================================
        console.log("\n--- CONSULTA 5: Heavy Users de Bad Bunny ---");

        const heavyUsersQuery = await db.collection('streams').aggregate([
            // 1. Traer info del artista desde la canción (o usar artist_id si supiéramos el ID de Bad Bunny)
            // Hacemos lookup a artists para filtrar por nombre "Bad Bunny"
            {
                $lookup: {
                    from: "artists",
                    localField: "artist_id",
                    foreignField: "_id",
                    as: "artist"
                }
            },
            { $unwind: "$artist" },
            { $match: { "artist.name": "Bad Bunny" } },
            // 2. Agrupar por usuario
            {
                $group: {
                    _id: "$user_id",
                    uniqueSongs: { $addToSet: "$song_id" } // addToSet solo guarda valores únicos
                }
            },
            // 3. Contar cuántas canciones únicas escuchó
            {
                $project: {
                    unique_songs_count: { $size: "$uniqueSongs" }
                }
            },
            // 4. Top 5
            { $sort: { unique_songs_count: -1 } },
            { $limit: 5 },
            // 5. Lookup para mostrar nombre del usuario
            {
                $lookup: {
                    from: "users",
                    localField: "_id",
                    foreignField: "_id",
                    as: "user_data"
                }
            },
            { $unwind: "$user_data" },
            {
                $project: {
                    username: "$user_data.username",
                    unique_bad_bunny_songs: "$unique_songs_count"
                }
            }
        ]).toArray();
        console.table(heavyUsersQuery);


    } catch (e) {
        console.error(e);
    } finally {
        await client.close();
    }
}

runQueries();