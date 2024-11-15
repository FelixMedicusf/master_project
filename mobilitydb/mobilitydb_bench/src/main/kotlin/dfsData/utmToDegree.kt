package geomesa.dfsData

import kotlin.math.*

// source: https://stackoverflow.com/questions/176137/java-convert-lat-lon-to-utm
class utmToDegree(UTM: String) {
    var latitude: Double
    var longitude: Double

    init {
        val parts = UTM.split(" ".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
        val zone = parts[0].toInt()
        val letter = parts[1].uppercase()[0]
        val easting = parts[2].toDouble()
        val northing = parts[3].toDouble()
        val hemisphere: Double
        hemisphere = if (letter > 'M') 'N'.code.toDouble() else 'S'.code.toDouble()
        val north: Double
        north = if (hemisphere == 'S'.code.toDouble()) northing - 10000000 else northing
        latitude =
            (north / 6366197.724 / 0.9996 + (1 + 0.006739496742 * cos(north / 6366197.724 / 0.9996).pow(2.0) - 0.006739496742 * sin(
                north / 6366197.724 / 0.9996
            ) * cos(north / 6366197.724 / 0.9996) * (atan(
                cos(
                    atan(
                        (exp(
                            (easting - 500000) / (0.9996 * 6399593.625 / sqrt(
                                1 + 0.006739496742 * cos(north / 6366197.724 / 0.9996).pow(2.0)
                            )) * (1 - 0.006739496742 * ((easting - 500000) / (0.9996 * 6399593.625 / sqrt(
                                1 + 0.006739496742 * cos(
                                    north / 6366197.724 / 0.9996
                                ).pow(2.0)
                            ))).pow(2.0) / 2 * cos(north / 6366197.724 / 0.9996).pow(2.0) / 3)
                        ) - exp(
                            -(easting - 500000) / (0.9996 * 6399593.625 / sqrt(
                                1 + 0.006739496742 * cos(north / 6366197.724 / 0.9996).pow(2.0)
                            )) * (1 - 0.006739496742 * ((easting - 500000) / (0.9996 * 6399593.625 / sqrt(
                                1 + 0.006739496742 * cos(
                                    north / 6366197.724 / 0.9996
                                ).pow(2.0)
                            ))).pow(2.0) / 2 * cos(north / 6366197.724 / 0.9996).pow(2.0) / 3)
                        )) / 2 / cos(
                            (north - 0.9996 * 6399593.625 * (north / 6366197.724 / 0.9996 - 0.006739496742 * 3 / 4 * (north / 6366197.724 / 0.9996 + sin(
                                2 * north / 6366197.724 / 0.9996
                            ) / 2) + (0.006739496742 * 3 / 4).pow(2.0) * 5 / 3 * (3 * (north / 6366197.724 / 0.9996 + sin(
                                2 * north / 6366197.724 / 0.9996
                            ) / 2) + sin(2 * north / 6366197.724 / 0.9996) * cos(north / 6366197.724 / 0.9996).pow(2.0)) / 4 - (0.006739496742 * 3 / 4).pow(
                                3.0
                            ) * 35 / 27 * (5 * (3 * (north / 6366197.724 / 0.9996 + sin(
                                2 * north / 6366197.724 / 0.9996
                            ) / 2) + sin(2 * north / 6366197.724 / 0.9996) * cos(north / 6366197.724 / 0.9996).pow(2.0)) / 4 + sin(
                                2 * north / 6366197.724 / 0.9996
                            ) * cos(north / 6366197.724 / 0.9996).pow(2.0) * cos(north / 6366197.724 / 0.9996).pow(2.0)) / 3)) / (0.9996 * 6399593.625 / sqrt(
                                1 + 0.006739496742 * cos(north / 6366197.724 / 0.9996).pow(2.0)
                            )) * (1 - 0.006739496742 * ((easting - 500000) / (0.9996 * 6399593.625 / sqrt(
                                1 + 0.006739496742 * cos(
                                    north / 6366197.724 / 0.9996
                                ).pow(2.0)
                            ))).pow(2.0) / 2 * cos(north / 6366197.724 / 0.9996).pow(2.0)) + north / 6366197.724 / 0.9996
                        )
                    )
                ) * tan(
                    (north - 0.9996 * 6399593.625 * (north / 6366197.724 / 0.9996 - 0.006739496742 * 3 / 4 * (north / 6366197.724 / 0.9996 + sin(
                        2 * north / 6366197.724 / 0.9996
                    ) / 2) + (0.006739496742 * 3 / 4).pow(2.0) * 5 / 3 * (3 * (north / 6366197.724 / 0.9996 + sin(
                        2 * north / 6366197.724 / 0.9996
                    ) / 2) + sin(2 * north / 6366197.724 / 0.9996) * cos(north / 6366197.724 / 0.9996).pow(2.0)) / 4 - (0.006739496742 * 3 / 4).pow(
                        3.0
                    ) * 35 / 27 * (5 * (3 * (north / 6366197.724 / 0.9996 + sin(
                        2 * north / 6366197.724 / 0.9996
                    ) / 2) + sin(2 * north / 6366197.724 / 0.9996) * cos(north / 6366197.724 / 0.9996).pow(2.0)) / 4 + sin(
                        2 * north / 6366197.724 / 0.9996
                    ) * cos(north / 6366197.724 / 0.9996).pow(2.0) * cos(north / 6366197.724 / 0.9996).pow(2.0)) / 3)) / (0.9996 * 6399593.625 / sqrt(
                        1 + 0.006739496742 * cos(north / 6366197.724 / 0.9996).pow(2.0)
                    )) * (1 - 0.006739496742 * ((easting - 500000) / (0.9996 * 6399593.625 / sqrt(
                        1 + 0.006739496742 * cos(
                            north / 6366197.724 / 0.9996
                        ).pow(2.0)
                    ))).pow(2.0) / 2 * cos(north / 6366197.724 / 0.9996).pow(2.0)) + north / 6366197.724 / 0.9996
                )
            ) - north / 6366197.724 / 0.9996) * 3 / 2) * (atan(
                cos(
                    atan(
                        (exp(
                            (easting - 500000) / (0.9996 * 6399593.625 / sqrt(
                                1 + 0.006739496742 * cos(north / 6366197.724 / 0.9996).pow(2.0)
                            )) * (1 - 0.006739496742 * ((easting - 500000) / (0.9996 * 6399593.625 / sqrt(
                                1 + 0.006739496742 * cos(
                                    north / 6366197.724 / 0.9996
                                ).pow(2.0)
                            ))).pow(2.0) / 2 * cos(north / 6366197.724 / 0.9996).pow(2.0) / 3)
                        ) - exp(
                            -(easting - 500000) / (0.9996 * 6399593.625 / sqrt(
                                1 + 0.006739496742 * cos(north / 6366197.724 / 0.9996).pow(2.0)
                            )) * (1 - 0.006739496742 * ((easting - 500000) / (0.9996 * 6399593.625 / sqrt(
                                1 + 0.006739496742 * cos(
                                    north / 6366197.724 / 0.9996
                                ).pow(2.0)
                            ))).pow(2.0) / 2 * cos(north / 6366197.724 / 0.9996).pow(2.0) / 3)
                        )) / 2 / cos(
                            (north - 0.9996 * 6399593.625 * (north / 6366197.724 / 0.9996 - 0.006739496742 * 3 / 4 * (north / 6366197.724 / 0.9996 + sin(
                                2 * north / 6366197.724 / 0.9996
                            ) / 2) + (0.006739496742 * 3 / 4).pow(2.0) * 5 / 3 * (3 * (north / 6366197.724 / 0.9996 + sin(
                                2 * north / 6366197.724 / 0.9996
                            ) / 2) + sin(2 * north / 6366197.724 / 0.9996) * cos(north / 6366197.724 / 0.9996).pow(2.0)) / 4 - (0.006739496742 * 3 / 4).pow(
                                3.0
                            ) * 35 / 27 * (5 * (3 * (north / 6366197.724 / 0.9996 + sin(
                                2 * north / 6366197.724 / 0.9996
                            ) / 2) + sin(2 * north / 6366197.724 / 0.9996) * cos(north / 6366197.724 / 0.9996).pow(2.0)) / 4 + sin(
                                2 * north / 6366197.724 / 0.9996
                            ) * cos(north / 6366197.724 / 0.9996).pow(2.0) * cos(north / 6366197.724 / 0.9996).pow(2.0)) / 3)) / (0.9996 * 6399593.625 / sqrt(
                                1 + 0.006739496742 * cos(north / 6366197.724 / 0.9996).pow(2.0)
                            )) * (1 - 0.006739496742 * ((easting - 500000) / (0.9996 * 6399593.625 / sqrt(
                                1 + 0.006739496742 * cos(
                                    north / 6366197.724 / 0.9996
                                ).pow(2.0)
                            ))).pow(2.0) / 2 * cos(north / 6366197.724 / 0.9996).pow(2.0)) + north / 6366197.724 / 0.9996
                        )
                    )
                ) * tan(
                    (north - 0.9996 * 6399593.625 * (north / 6366197.724 / 0.9996 - 0.006739496742 * 3 / 4 * (north / 6366197.724 / 0.9996 + sin(
                        2 * north / 6366197.724 / 0.9996
                    ) / 2) + (0.006739496742 * 3 / 4).pow(2.0) * 5 / 3 * (3 * (north / 6366197.724 / 0.9996 + sin(
                        2 * north / 6366197.724 / 0.9996
                    ) / 2) + sin(2 * north / 6366197.724 / 0.9996) * cos(north / 6366197.724 / 0.9996).pow(2.0)) / 4 - (0.006739496742 * 3 / 4).pow(
                        3.0
                    ) * 35 / 27 * (5 * (3 * (north / 6366197.724 / 0.9996 + sin(
                        2 * north / 6366197.724 / 0.9996
                    ) / 2) + sin(2 * north / 6366197.724 / 0.9996) * cos(north / 6366197.724 / 0.9996).pow(2.0)) / 4 + sin(
                        2 * north / 6366197.724 / 0.9996
                    ) * cos(north / 6366197.724 / 0.9996).pow(2.0) * cos(north / 6366197.724 / 0.9996).pow(2.0)) / 3)) / (0.9996 * 6399593.625 / sqrt(
                        1 + 0.006739496742 * cos(north / 6366197.724 / 0.9996).pow(2.0)
                    )) * (1 - 0.006739496742 * ((easting - 500000) / (0.9996 * 6399593.625 / sqrt(
                        1 + 0.006739496742 * cos(
                            north / 6366197.724 / 0.9996
                        ).pow(2.0)
                    ))).pow(2.0) / 2 * cos(north / 6366197.724 / 0.9996).pow(2.0)) + north / 6366197.724 / 0.9996
                )
            ) - north / 6366197.724 / 0.9996)) * 180 / Math.PI
        latitude = Math.round(latitude * 10000000).toDouble()
        latitude = latitude / 10000000
        longitude = atan(
            (exp(
                (easting - 500000) / (0.9996 * 6399593.625 / sqrt(
                    1 + 0.006739496742 * cos(north / 6366197.724 / 0.9996).pow(
                        2.0
                    )
                )) * (1 - 0.006739496742 * ((easting - 500000) / (0.9996 * 6399593.625 / sqrt(
                    1 + 0.006739496742 * cos(north / 6366197.724 / 0.9996).pow(2.0)
                ))).pow(2.0) / 2 * cos(north / 6366197.724 / 0.9996).pow(2.0) / 3)
            ) - exp(
                -(easting - 500000) / (0.9996 * 6399593.625 / sqrt(
                    1 + 0.006739496742 * cos(north / 6366197.724 / 0.9996).pow(2.0)
                )) * (1 - 0.006739496742 * ((easting - 500000) / (0.9996 * 6399593.625 / sqrt(
                    1 + 0.006739496742 * cos(
                        north / 6366197.724 / 0.9996
                    ).pow(2.0)
                ))).pow(2.0) / 2 * cos(north / 6366197.724 / 0.9996).pow(2.0) / 3)
            )) / 2 / cos(
                (north - 0.9996 * 6399593.625 * (north / 6366197.724 / 0.9996 - 0.006739496742 * 3 / 4 * (north / 6366197.724 / 0.9996 + sin(
                    2 * north / 6366197.724 / 0.9996
                ) / 2) + (0.006739496742 * 3 / 4).pow(2.0) * 5 / 3 * (3 * (north / 6366197.724 / 0.9996 + sin(
                    2 * north / 6366197.724 / 0.9996
                ) / 2) + sin(2 * north / 6366197.724 / 0.9996) * cos(north / 6366197.724 / 0.9996).pow(2.0)) / 4 - (0.006739496742 * 3 / 4).pow(
                    3.0
                ) * 35 / 27 * (5 * (3 * (north / 6366197.724 / 0.9996 + sin(
                    2 * north / 6366197.724 / 0.9996
                ) / 2) + sin(2 * north / 6366197.724 / 0.9996) * cos(north / 6366197.724 / 0.9996).pow(2.0)) / 4 + sin(
                    2 * north / 6366197.724 / 0.9996
                ) * cos(north / 6366197.724 / 0.9996).pow(2.0) * cos(north / 6366197.724 / 0.9996).pow(2.0)) / 3)) / (0.9996 * 6399593.625 / sqrt(
                    1 + 0.006739496742 * cos(north / 6366197.724 / 0.9996).pow(2.0)
                )) * (1 - 0.006739496742 * ((easting - 500000) / (0.9996 * 6399593.625 / sqrt(
                    1 + 0.006739496742 * cos(
                        north / 6366197.724 / 0.9996
                    ).pow(2.0)
                ))).pow(2.0) / 2 * cos(north / 6366197.724 / 0.9996).pow(2.0)) + north / 6366197.724 / 0.9996
            )
        ) * 180 / Math.PI + zone * 6 - 183
        longitude = Math.round(longitude * 10000000).toDouble()
        longitude = longitude / 10000000
    }
}
