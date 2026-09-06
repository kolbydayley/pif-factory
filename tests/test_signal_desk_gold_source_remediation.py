from research_factory.signal_desk_gold_source_remediation import diagnose_source

def test_implausibly_long_text_is_episode_mismatch():
 assert diagnose_source(show_id='s',source_kind='youtube_captions',source_url='u',text='word '*1000,duration_seconds=60)=='episode_transcript_mismatch'

def test_github_page_is_not_a_transcript():
 assert diagnose_source(show_id='s',source_kind='creator_provided_rss_transcript',source_url='https://github.com/x/y.md',text='Skip to content Navigation Menu '+('word '*500),duration_seconds=600)=='non_transcript_source_document'

def test_short_page_extract_is_incomplete():
 assert diagnose_source(show_id='s',source_kind='official',source_url='u',text='word '*100,duration_seconds=1800)=='incomplete_transcript_or_page_extract'

def test_collapsed_generic_labels_require_diarization():
 assert diagnose_source(show_id='s',source_kind='rss',source_url='u',text=('Speaker 1: words here\n'*300),duration_seconds=600)=='collapsed_generic_speaker_labels'

def test_named_labels_leave_only_unlabeled_narration_problem():
 assert diagnose_source(show_id='s',source_kind='official',source_url='u',text=('Alice Smith: words here\n'*300),duration_seconds=600)=='unlabeled_narration_before_named_turns'
