from __future__ import annotations

import json

from research_factory.signal_desk_rebuild_ood_acquisition import (
    acquire_show,
    extract_official_page,
)


def test_official_air_date_overrides_bulk_cms_migration_timestamp() -> None:
    transcript = "</p><p>".join(
        " ".join(f"word{section}_{index}" for index in range(180))
        for section in range(6)
    )
    html = f"""
      <html><head>
        <meta property="article:published_time" content="2026-05-05T01:04:00Z">
        <meta property="og:title" content="A migrated transcript">
      </head><body><main>
        <p>Original Air Date April 18, 2017</p>
        <h2>Transcript</h2><section><p>{transcript}</p></section>
      </main></body></html>
    """.encode()
    page = extract_official_page(html, url="https://first-party.test/transcript")
    assert page["published_at"] == "2017-04-18T00:00:00+00:00"
    assert page["word_count"] >= 900


def test_this_american_life_transcript_header_beats_migration_date() -> None:
    transcript = "</p><p>".join(
        " ".join(f"word{section}_{index}" for index in range(180))
        for section in range(6)
    )
    html = f"""
      <html><head>
        <meta property="article:published_time" content="2026-08-31T01:04:00Z">
        <meta property="og:title" content="397: 2010">
      </head><body><article>
        <p>Transcript | Episode #397 | January 1, 2010</p>
        <h2>Transcript</h2><section><p>{transcript}</p></section>
      </article></body></html>
    """.encode()
    page = extract_official_page(html, url="https://first-party.test/397/transcript")
    assert page["published_at"] == "2010-01-01T00:00:00+00:00"


def test_sealed_acquisition_emits_no_transcript_text(monkeypatch, tmp_path, capsys) -> None:
    sentinel = "SEALED_BODY_SENTINEL_MUST_NOT_LEAK"

    def fake_fetch(url, *, allowed_hosts):
        index = int(url.rsplit("/", 1)[-1])
        paragraphs = "".join(
            "<p>" + " ".join(
                [sentinel, *[f"word{index}_{section}_{word}" for word in range(180)]]
            ) + "</p>"
            for section in range(6)
        )
        return f"""
          <html><head>
            <meta property="og:title" content="Fixture {index}">
            <meta property="article:published_time" content="202{index}-01-01T00:00:00Z">
          </head><body><main><h2>Transcript</h2>{paragraphs}</main></body></html>
        """.encode()

    monkeypatch.setattr(
        "research_factory.signal_desk_rebuild_ood_acquisition._fetch", fake_fetch
    )
    canonical = tmp_path / "canonical" / "factory.sqlite"
    canonical.parent.mkdir()
    canonical.touch()
    receipt = acquire_show(
        {
            "show_id": "sealed-fixture",
            "show_name": "Sealed Fixture",
            "source_shape": "narrative",
            "sealed": True,
            "first_party_host": "first-party.test",
            "official_urls": [f"https://first-party.test/{index}" for index in range(4)],
        },
        output_root=tmp_path / "fixtures",
        benchmark_database=tmp_path / "private-benchmark" / "fixtures.sqlite",
        canonical_database=canonical,
    )
    assert capsys.readouterr().out == ""
    assert sentinel not in json.dumps(receipt)
    assert receipt["privacy"] == "sealed_transcript_text_not_logged_or_embedded_in_receipt"
