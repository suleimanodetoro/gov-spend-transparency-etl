import pipeline as P


def test_manifest_records_and_detects_processed_files(tmp_path):
    f = tmp_path / "a.csv"
    f.write_text("some content")
    h = P.file_hash(str(f))
    manifest = tmp_path / "manifest.csv"

    assert P.load_manifest(str(manifest)) == set()      # nothing processed yet
    P.append_manifest(str(manifest), [["a.csv", h, 10, 0, "t"]])
    assert h in P.load_manifest(str(manifest))           # now skipped on re-run


def test_source_key_from_filename():
    assert P.source_key("/x/cabinet_office_spend_2026_05_schemadrift.csv") == "cabinet_office"
    assert P.source_key("/x/mod_spend_2026_05_malformed.csv") == "mod"
    assert P.source_key("/x/dhsc_spend_2026_04.csv") == "dhsc"
