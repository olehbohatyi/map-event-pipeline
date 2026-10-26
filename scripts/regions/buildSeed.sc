//> using scala 3.10.0
//> using dep com.lihaoyi::os-lib:0.11.9
//> using dep com.lihaoyi::ujson:4.4.3

// Generates core/src/main/resources/db/migration/V3__regions_ukraine.sql from Natural Earth.
//
//   scala-cli run scripts/regions/buildSeed.sc        (from the repository root; needs Docker)
//
// Steps: download Natural Earth admin-1 (pinned release) → keep the Ukrainian regions → load them into a
// throwaway PostGIS container → simplify with ST_CoverageSimplify (shared borders stay shared) → check the result
// → write one INSERT per region. The migration is committed; this script is how it was made and how to remake it
// (as a new V<n> migration — applied migrations must never change).

val NaturalEarthTag = "v5.1.2"
val NaturalEarthUrl =
  s"https://raw.githubusercontent.com/nvkelso/natural-earth-vector/$NaturalEarthTag/geojson/ne_10m_admin_1_states_provinces.geojson"
val PostgisImage = "postgis/postgis:18-3.6-alpine"
val Container    = "mep-regions-seed"
val Tolerance    = 0.003 // degrees, ~300 m: simplification strength
val Decimals     = 5     // coordinate precision of the output, ~1 m
val Output       = os.pwd / "core" / "src" / "main" / "resources" / "db" / "migration" / "V3__regions_ukraine.sql"

// Curated display names, keyed by ISO 3166-2 code. Natural Earth calls both Kyiv entries "Kyiv" and spells
// some names differently ("Odessa"), so names are fixed here and only the geometry comes from the dataset.
val Names: Map[String, String] = Map(
  "UA-05" -> "Vinnytsia Oblast",
  "UA-07" -> "Volyn Oblast",
  "UA-09" -> "Luhansk Oblast",
  "UA-12" -> "Dnipropetrovsk Oblast",
  "UA-14" -> "Donetsk Oblast",
  "UA-18" -> "Zhytomyr Oblast",
  "UA-21" -> "Zakarpattia Oblast",
  "UA-23" -> "Zaporizhzhia Oblast",
  "UA-26" -> "Ivano-Frankivsk Oblast",
  "UA-30" -> "Kyiv City",
  "UA-32" -> "Kyiv Oblast",
  "UA-35" -> "Kirovohrad Oblast",
  "UA-40" -> "Sevastopol City",
  "UA-43" -> "Autonomous Republic of Crimea",
  "UA-46" -> "Lviv Oblast",
  "UA-48" -> "Mykolaiv Oblast",
  "UA-51" -> "Odesa Oblast",
  "UA-53" -> "Poltava Oblast",
  "UA-56" -> "Rivne Oblast",
  "UA-59" -> "Sumy Oblast",
  "UA-61" -> "Ternopil Oblast",
  "UA-63" -> "Kharkiv Oblast",
  "UA-65" -> "Kherson Oblast",
  "UA-68" -> "Khmelnytskyi Oblast",
  "UA-71" -> "Cherkasy Oblast",
  "UA-74" -> "Chernihiv Oblast",
  "UA-77" -> "Chernivtsi Oblast",
)

// Throws instead of exiting, so the cleanup in `finally` below still removes the container
def fail(msg: String): Nothing = throw RuntimeException(s"buildSeed: $msg")

def sh(args: String*): os.CommandResult = os.proc(args).call(check = false, mergeErrIntoOut = true)

// ── 1. Natural Earth → the 27 Ukrainian regions ──────────────────────────────────────────────────────────────
val workDir = os.temp.dir(prefix = "mep-regions-")
val source  = workDir / "admin1.geojson"
println(s"Downloading Natural Earth $NaturalEarthTag admin-1 (~40 MB) …")
val download = sh("curl", "-fsSL", "--max-time", "600", "-o", source.toString, NaturalEarthUrl)
if download.exitCode != 0 then fail(s"download failed:\n${download.out.text()}")

// Selected by ISO code, not by country: Natural Earth's default data files Crimea (UA-43) and Sevastopol (UA-40)
// under another country, but they are Ukrainian regions.
val features = ujson.read(os.read(source))("features").arr.filter(_("properties")("iso_3166_2").strOpt.exists(_.startsWith("UA-")))
val codes    = features.map(_("properties")("iso_3166_2").str).toSet
if codes != Names.keySet then fail(s"expected ${Names.keySet.toList.sorted}, found ${codes.toList.sorted}")
println(s"Selected ${features.size} regions by ISO 3166-2 code")

// ── 2. PostGIS: load, simplify, verify, render INSERTs ───────────────────────────────────────────────────────
def psql(sql: String): String =
  val r = os.proc("docker", "exec", "-i", Container, "psql", "-U", "postgres", "-v", "ON_ERROR_STOP=1", "-At", "-q", "-f", "-")
    .call(stdin = sql, check = false, mergeErrIntoOut = true)
  if r.exitCode != 0 then fail(s"psql failed:\n${r.out.text()}")
  r.out.text()

sh("docker", "rm", "-f", Container)
val started = sh("docker", "run", "-d", "--rm", "--name", Container, "-e", "POSTGRES_PASSWORD=seed", PostgisImage)
if started.exitCode != 0 then fail(s"cannot start $PostgisImage:\n${started.out.text()}")

try
  // The image restarts Postgres once after initialising; only the final server listens on TCP
  val ready = (1 to 60).exists { _ =>
    Thread.sleep(2000)
    sh("docker", "exec", Container, "psql", "-h", "127.0.0.1", "-U", "postgres", "-Atc", "select 1").exitCode == 0
  }
  if !ready then fail("PostGIS did not become ready")

  val load = new StringBuilder("CREATE EXTENSION IF NOT EXISTS postgis;\nCREATE TABLE names (iso text PRIMARY KEY, name text NOT NULL);\n")
  load ++= "CREATE TABLE raw (iso text PRIMARY KEY, geom geometry(MultiPolygon, 4326) NOT NULL);\n"
  Names.toList.sorted.foreach((iso, name) => load ++= s"INSERT INTO names VALUES ('$iso', '${name.replace("'", "''")}');\n")
  features.foreach { f =>
    val geometry = ujson.write(f("geometry")).replace("'", "''")
    load ++= s"INSERT INTO raw VALUES ('${f("properties")("iso_3166_2").str}', ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON('$geometry'), 4326)));\n"
  }
  psql(load.toString)

  // Simplify as one coverage, then round-trip through GeoJSON at the output precision so the checks below run on
  // exactly what will be committed
  psql(s"""
    CREATE TABLE simplified AS
      SELECT iso, ST_Multi(ST_CoverageSimplify(geom, $Tolerance) OVER ())::geometry(MultiPolygon, 4326) AS geom FROM raw;
    CREATE TABLE final AS
      SELECT iso, ST_Multi(ST_GeomFromGeoJSON(ST_AsGeoJSON(geom, $Decimals)))::geometry(MultiPolygon, 4326) AS geom FROM simplified;
  """)

  // Quality gates: refuse to write a seed that has holes, overlaps or broken geometry
  val checks = psql("""
    SELECT 'regions', count(*) FROM final;
    SELECT 'invalid geometries', count(*) FROM final WHERE NOT ST_IsValid(geom);
    SELECT 'coverage gaps or overlaps', count(*) FROM (SELECT ST_CoverageInvalidEdges(geom) OVER () AS e FROM final) c WHERE e IS NOT NULL;
    SELECT 'max area change %', round(max(abs(ST_Area(f.geom) - ST_Area(r.geom)) / ST_Area(r.geom) * 100)::numeric, 4)
      FROM final f JOIN raw r USING (iso);
    SELECT 'points before / after', (SELECT sum(ST_NPoints(geom)) FROM raw) || ' / ' || (SELECT sum(ST_NPoints(geom)) FROM final);
  """)
  println(checks.linesIterator.map(l => "  " + l.replace("|", ": ")).mkString("\n"))
  val result = checks.linesIterator.map(_.split('|')).collect { case Array(k, v) => k -> v }.toMap
  if result("regions") != "27" then fail("expected 27 regions")
  if result("invalid geometries") != "0" then fail("invalid geometries after simplification")
  if result("coverage gaps or overlaps") != "0" then fail("simplification opened gaps or overlaps between regions")

  val inserts = psql(
    """SELECT format('INSERT INTO regions (code, name, country, geom) VALUES (%L, %L, %L, ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON(%L), 4326)));',
      |              f.iso, n.name, 'UA', ST_AsGeoJSON(f.geom, 5))
      |FROM final f JOIN names n USING (iso) ORDER BY f.iso;""".stripMargin
  )

  val header =
    s"""-- Ukrainian first-level regions: 24 oblasts, the Autonomous Republic of Crimea, and the cities of Kyiv and Sevastopol.
       |--
       |-- Source: Natural Earth 1:10m Admin 1 - States, Provinces, $NaturalEarthTag (public domain, https://www.naturalearthdata.com/).
       |-- Selected by ISO 3166-2 code (UA-*): Natural Earth's default data files Crimea and Sevastopol under another country.
       |-- Geometry: ST_CoverageSimplify with a $Tolerance degree tolerance (shared borders stay shared), $Decimals decimal places.
       |-- Display names are curated; the dataset only supplies the geometry.
       |--
       |-- Generated by scripts/regions/buildSeed.sc - do not edit. To change the data, generate a new migration.
       |""".stripMargin
  os.write.over(Output, header + "\n" + inserts, createFolders = true)
  println(s"Wrote $Output (${os.size(Output) / 1024} KB)")
finally
  sh("docker", "rm", "-f", Container)
  os.remove.all(workDir)
