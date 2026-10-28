from enum import Enum


class Error(int, Enum):
    LLM_AUTHENTICATION_ERROR = 401
    LLM_PERMISSION_DENIED = 403
    LLM_QUOTA_LIMIT = 429
    LLM_PROVIDERS_UNKNOWN_ERROR = 500
    LLM_PROVIDERS_UNAVAILABLE = 503
    NETWORK_ERROR = 600
    READ_PAPER_ERROR = 601  # error_message: file name
    READ_PAPER_ERROR_FILE_TYPE = 602  # error_message: file name | file type
    INVALID_PARAMS = 603
    TIME_OUT_REQUEST = 604
    NOT_IMPLEMENT_MODEL = 605
    EVENT_NOT_FOUND = 606
    MAXIMUM_RETRY = 607
    UNKNOWN = 608
    MAX_OUTPUT_TOKENS = 609
    INVALID_SEARCH_KEY = 701  # Wrong search api key
    SEARCH_QUOTA_LIMIT = 702  # Search Usage limit reach
    MISSING_SEARCH_KEY = 703  # Missing search api key
    SEARCH_ERROR = 704  # Wrong search body error
    SEARCH_UNKNOWN = 700  # Unknown search error
    
    
class DocumentSetupError(int, Enum):
    SEARCH_PAPER_ERROR = 610
    GENERATE_DOMAINS = 611
    GENERATE_SUBDOMAINS = 612
    GENERATE_KEYWORDS = 613
    GENERATE_TITLES = 614
    GENERATE_TITLE_DESCRIPTION = 615
    MIX_TITLES = 616
    GENERATE_OUTLINE = 617
    ADMIN_DOCUMENTS = 618
    USER_GUIDE_DOCUMENTS = 619
    USER_DOCUMENTS = 620
    EMBED_USER_DOCUMENTS = 621
    DELETE_DOCUMENTS = 622
    GET_RESEARCH_TYPE = 623
    GENERATE_TITLE_DESCRIPTION_NO_INFO = 624
    SEARCH_USER_DOCUMENTS = 625
    GENERATE_OUTLINE_WITH_REFS = 626
    MODIFY_OUTLINE = 627
    UPDATE_USER_REFS = 628


class WriteSectionError(int, Enum):
    WRITE_CONTENT = 630
    ANALYZER = 631
    CHUYEN_DE_1 = 632
    CHUYEN_DE_2 = 633
    GEN_SLIDES = 634
    CHUYEN_DE_3 = 635
    COMMENT_DATA = 636
    PROPOSE_METHOD = 637
    PARSED_VALIDATION = 638
    PARSED_VALUE = 639
    DELETE_REPORT = 640
    TRANSLATE_REPORT = 641
    CHECK_LOGIC_DATA = 642
    EDIT_REPORT = 643


class ChatbotError(int, Enum):
    OUTER_CHATBOT = 650
    INNER_CHATBOT = 651
    WRITER_CHATBOT = 652
    WRITER = 653


class EnhancementError(int, Enum):
    AI_IN_DOC = 670
    GET_SUGGESTIONS = 671
    ENHANCE = 672
    ENHANCE_ALL = 673


ERROR_DICT = {
    # DOCUMENT SETUP
    "search_paper_error": 610,
    "generate_domains": 611,
    "generate_subdomains": 612,
    "generate_keywords": 613,
    "generate_titles": 614,
    "generate_title_description": 615,
    "mix_titles": 616,
    "generate_outline": 617,
    "admin_documents": 618,
    "user_guide_documents": 619,
    "user_documents": 620,
    "embed_user_documents": 621,
    "delete_documents": 622,
    "get_research_type": 623,
    "generate_title_description_no_info": 624,
    "search_user_documents": 625,
    "generate_outline_with_refs": 626,
    "modify_outline": 627,
    "update_user_refs": 628,
    # WRITE SECTION
    "write_content": 630,
    "analyzer": 631,
    "chuyen_de_1": 632,
    "chuyen_de_2": 633,
    "gen_slides": 634,
    "chuyen_de_3": 635,
    "comment_data": 636,
    "propose_method": 637,
    "parsed_validation": 638,
    "parsed_value": 639,
    "delete_report": 640,
    "translate_report": 641,
    "check_logic_data": 642,
    "edit_report": 643,
    # CHATBOT
    "outer_chatbot": 650,
    "inner_chatbot": 651,
    "writer_chatbot": 652,
    "writer": 653,
    # ENHANCEMENT
    "ai_in_doc": 670,
    "get_suggestions": 671,
    "enhance": 672,
    "enhance_all": 673
}

TIME_OUT_DICT = {
    # DOCUMENT SETUP
    "search_papers": 180,
    "generate_domains": 60,
    "generate_subdomains": 60,
    "generate_keywords": 60,
    "generate_titles": 300,
    "generate_title_description": 300,
    "mix_titles": 300,
    "generate_outline": 1200,
    "admin_documents": 0,
    "user_guide_documents": 0,
    "user_documents": 0,
    "embed_user_documents": 0,
    "get_journals": 900,
    "get_research_type": 60,
    # WRITE SECTION
    "write_content": 1500,
    "analyzer": 300,
    "analyzer_tool": 300,
    "chuyen_de_1": 1500,
    "chuyen_de_2": 1500,
    "gen_slides": 300,
    "chuyen_de_3": 1500,
    "comment_data": 300,
    "propose_method": 300,
    "parsed_value": 300,
    "edit_report": 300,
    # CHATBOT
    "outer_chatbot": 120,
    "inner_chatbot": 120,
    "writer_chatbot": 120,
    "writer": 1500,
    # ENHANCEMENT
    "ai_in_doc": 120,
    "get_suggestions": 120,
    "enhance": 120,
    "enhance_all": 120
}

MAX_TOKENS_DICT = {
    # DOCUMENT SETUP
    "search_papers": 500,
    "generate_domains": 500,
    "generate_subdomains": 500,
    "generate_keywords": 500,
    "generate_titles": 5000,
    "generate_title_description": 2000,
    "mix_titles": 2000,
    "generate_outline": 2000,
    "admin_documents": 5000,
    "user_documents": 10000,
    "get_research_type": 500,
    "generate_title_description_no_info": 2000,
    "search_user_documents": 2000,
    "generate_outline_with_refs": 5000,
    "update_user_refs": 5000,
    # WRITE SECTION
    "analyzer": 2000,
    "analyzer_tool": 2000,
    "gen_slides": 5000,
    "comment_data": 2000,
    "propose_method": 5000,
    "translate_report": 5000,
    "edit_report": 5000,
    "parsed_value": 5000,
    # CHATBOT
    "outer_chatbot": 2000,
    "inner_chatbot": 2000,
    "writer_chatbot": 5000,
    # ENHANCEMENT
    "ai_in_doc": 2000,
    "get_suggestions": 2000,
    "enhance": 1000,
}