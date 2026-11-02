DOCTERATE_OUTLINE_PROMPT = """
You're a PhD professor, you task is to help the student generate subheadings for student docterate thesis based on final proposal. 
Generate the outline for thesis, including headings, sub-headings (generate upto 4 subheading level, for example: 1.3.4.2), and its detailed description and word limit. 
IMPORTANT: DO NOT CHANGE THE `headings`, keep it as it is
Return just JSON object
Return in user's language
"""

SUBHEADINGS_DESCRIPTION_PROMPT = """
You're a PhD professor, with many years of academic writing.
You will be give a final proposal and the corresponding outline.
You task is to help the student generate description for subheadings for student docterate thesis based on final proposal and outline so that the student can based on that to write accordingly. 
Generate the subheading description and its word limit.
IMPORTANT: DO NOT CHANGE THE INPUT `headings` and `subheadings`, keep it as it is
For example:
- Input:
```
{"final_proposal": {
    "title": "Phát Triển Framework Tích Hợp AI Sinh Tạo và Phân Tích Big Data Thời Gian Thực cho Quản Lý Rủi Ro Chuỗi Cung Ứng trong Thương Mại Điện Tử dưới Khủng Hoảng Toàn Cầu",
    "problem_statement": "Các mô hình hiện tại chưa thể xử lý hiệu quả và tích hợp đầy đủ các nguồn dữ liệu đa chiều, thời gian thực để dự báo rủi ro và đưa ra quyết định thích ứng nhanh chóng trong quản lý chuỗi cung ứng cho các doanh nghiệp thương mại điện tử trong bối cảnh khủng hoảng toàn cầu, đặc biệt khi đối mặt với các biến động phức tạp và không chắc chắn như đại dịch COVID-19. Vấn đề này rất quan trọng vì chuỗi cung ứng bị gián đoạn sẽ ảnh hưởng nghiêm trọng đến kinh doanh, làm giảm khả năng cạnh tranh, gây thiệt hại kinh tế và đe dọa khả năng phục hồi của doanh nghiệp.",
    "motivation": "Các phương pháp hiện nay chủ yếu dựa trên việc sử dụng các kỹ thuật phân tích big data truyền thống kết hợp với các mô hình học máy để dự báo rủi ro trong chuỗi cung ứng nhưng tập trung chủ yếu vào dữ liệu lịch sử và các chỉ số tĩnh, thiếu khả năng tích hợp dữ liệu đa chiều thời gian thực, đồng thời thiếu các cơ chế thích ứng chủ động và linh hoạt trong phản ứng với biến động nhanh của thị trường và các sự kiện bất ngờ. Cảm hứng phát triển phương pháp mới đến từ việc tận dụng sức mạnh của AI sinh tạo (generative AI) với khả năng mô phỏng và tạo ngữ cảnh phức tạp cùng công nghệ phân tích big data đa chiều thời gian thực để không chỉ dự báo kịp thời mà còn đề xuất, điều chỉnh các quyết định chủ động trong chuỗi cung ứng, hứa hẹn nâng cao khả năng bền vững và năng lực cạnh tranh của doanh nghiệp thương mại điện tử trong khủng hoảng.",
    "proposed_method": "Phương pháp đề xuất là xây dựng một framework tích hợp AI sinh tạo với phân tích big data thời gian thực cho quản lý chuỗi cung ứng trong thương mại điện tử. Framework gồm các bước sau: (1) Thu thập dữ liệu đa dạng và liên tục từ nhiều nguồn: dữ liệu thị trường (giá cả, đơn hàng), logistics (tình trạng vận chuyển, tồn kho), hành vi khách hàng (mua sắm, phản hồi), mạng xã hội, và tin tức thời gian thực liên quan; (2) Xây dựng mô-đun AI sinh tạo dựa trên mô hình transformer kết hợp mô phỏng agent-based để phân tích dữ liệu đầu vào, dự báo rủi ro và mô phỏng các kịch bản tương tác trong chuỗi cung ứng nhằm nhận diện các nguy cơ và xu hướng nhanh chóng; (3) Thiết kế thành phần ra quyết định thích ứng tự động, sử dụng đầu ra dự báo để tối ưu hóa các chiến lược tồn kho, vận chuyển, lựa chọn nhà cung cấp sao cho cân bằng giữa mục tiêu về tính bền vững, chi phí và thời gian; (4) Thiết lập vòng lặp phản hồi liên tục để cập nhật mô hình dựa trên dữ liệu thực tế mới, đánh giá hiệu quả giải pháp và điều chỉnh kịch bản cũng như chiến lược thích ứng. Cách tiếp cận này khai thác ưu thế mô phỏng đa chiều, khả năng tạo kịch bản phức tạp của AI sinh tạo, giúp quản lý chuỗi cung ứng chủ động, linh hoạt và hiệu quả hơn so với các mô hình học máy truyền thống dựa trên dữ liệu lịch sử tĩnh.",
    "experiment_plan": "Giai đoạn 1 - Thu thập dữ liệu: Thu thập dữ liệu thực tế từ các doanh nghiệp thương mại điện tử trong giai đoạn đại dịch COVID-19, bao gồm dữ liệu logistics (vận chuyển, tồn kho), bán hàng, thị trường (giá cả, đơn hàng), hành vi khách hàng và dữ liệu mạng xã hội cũng như tin tức liên quan đến các sự kiện bất ổn. Chuẩn hóa và tiền xử lý dữ liệu để đảm bảo tính đồng bộ và chất lượng dữ liệu cho phân tích sau này. Giai đoạn 2 - Phát triển mô hình: Xây dựng mô hình AI sinh tạo (transformer + agent-based simulation) tích hợp phân tích big data đa chiều để dự báo rủi ro chuỗi cung ứng và đề xuất các chiến lược thích ứng. Huấn luyện mô hình bằng dữ liệu đã thu thập, điều chỉnh các siêu tham số để đạt hiệu quả tối ưu. Giai đoạn 3 - Đánh giá so sánh: So sánh chất lượng dự báo rủi ro và hiệu quả quản lý chuỗi cung ứng của mô hình đề xuất với các baseline hiện có như Random Forest, LSTM, và hệ thống quản lý chuỗi cung ứng tiêu chuẩn. Sử dụng các chỉ số đánh giá gồm thời gian phản ứng trước rủi ro, độ chính xác dự báo, chi phí vận hành, mức độ bền vững (tiêu chí môi trường và xã hội). Giai đoạn 4 - Thử nghiệm mô phỏng: Tạo các kịch bản khủng hoảng cụ thể (ví dụ: đứt gãy nguồn cung, tăng đột biến đơn hàng, biến động tài chính) để đánh giá khả năng thích ứng và tối ưu chiến lược của mô hình. Quan sát khả năng cập nhật và phản hồi của hệ thống trong thời gian thực. Giai đoạn 5 - Phân tích kết quả và hoàn thiện: Phân tích kết quả đánh giá trên các tiêu chí đã định, đưa ra nhận xét về điểm mạnh, hạn chế và khả năng áp dụng thực tế của framework. Hoàn thiện phương pháp và đề xuất các hướng ứng dụng thực tiễn cho doanh nghiệp thương mại điện tử nhằm nâng cao tính bền vững và khả năng cạnh tranh trong bối cảnh khủng hoảng toàn cầu.",
    "web_search": "**1. Review of Existing Literature on Big Data Analytics Applications in Supply Chain Risk Management During Global Crises**\n\nThe COVID-19 pandemic underscored the critical role of big data analytics in enhancing supply chain resilience. Studies have demonstrated that big data analytics capabilities significantly contribute to supply chain risk management and the development of innovative green products, thereby improving overall performance. ([emerald.com](https://www.emerald.com/insight/content/doi/10.1108/ijoem-12-2021-1807/full/html?utm_source=openai)) Additionally, AI-driven data collection and analysis methods have been identified as effective strategies for mitigating supply chain disruptions during pandemics. ([drpress.org](https://drpress.org/ojs/index.php/HBEM/article/view/13039?utm_source=openai))\n\n**2. Analysis of State-of-the-Art Methodologies Integrating Big Data Analytics with Supply Chain Risk Management Focused on E-Commerce Sectors**\n\nIn the e-commerce sector, integrating big data analytics with supply chain risk management has been pivotal in enhancing resilience. A study focusing on Emirati companies found that big data analytics capabilities positively influence supply chain preparedness and agility, which are essential for digital transformation initiatives. ([tandfonline.com](https://www.tandfonline.com/doi/full/10.1080/13675567.2022.2052825?utm_source=openai)) Furthermore, AI and big data analytics have been recognized for their potential to improve supply chain resilience by facilitating more effective management of resources. ([pubmed.ncbi.nlm.nih.gov](https://pubmed.ncbi.nlm.nih.gov/36212520/?utm_source=openai))\n\n**3. Investigation of Economic Impacts of Supply Chain Disruptions and the Role of Digital Transformation and AI-Driven Analytics in Enhancing Supply Chain Flexibility and Sustainability**\n\nSupply chain disruptions, such as those caused by the COVID-19 pandemic, have significant economic impacts, including increased food insecurity and economic instability. ([time.com](https://time.com/6182686/sara-menker-food-insecurity/?utm_source=openai)) Digital transformation, particularly through AI-driven analytics, plays a crucial role in enhancing supply chain flexibility and sustainability. A framework combining AI, blockchain, and big data analytics has been proposed to leverage supply chain resilience during disruptions. ([pubmed.ncbi.nlm.nih.gov](https://pubmed.ncbi.nlm.nih.gov/38620980/?utm_source=openai))\n\n**4. Examination of Policy, Ethical, and Legal Considerations Related to Using AI and Big Data in Supply Chain Risk Management Within the Context of Digital Economics**\n\nThe integration of AI and big data in supply chain risk management raises several policy, ethical, and legal considerations. The OECD has cautioned against aggressive reshoring of supply chains in response to geopolitical tensions, as it could significantly hurt global trade and GDP. ([ft.com](https://www.ft.com/content/e930fdce-367c-4e23-9967-9181b5cf43bc?utm_source=openai)) Additionally, the use of AI and big data in supply chains necessitates careful consideration of data privacy, security, and regulatory compliance to ensure ethical and legal adherence.\n\n**5. Synthesis of Findings to Propose Data-Driven, Integrated Models for Optimizing Supply Chain Resilience and Risk Management in E-Commerce During Global Crises, Highlighting Economic Benefits and Practical Applications**\n\nIntegrating big data analytics with supply chain risk management offers substantial economic benefits, including enhanced resilience and competitiveness. A study examining the link between green supply chain management practices and competitiveness during COVID-19 found that big data analytics and artificial intelligence strengthen the positive relationship between environmental management systems and market competitiveness. ([pubmed.ncbi.nlm.nih.gov](https://pubmed.ncbi.nlm.nih.gov/36090699/?utm_source=openai)) Implementing data-driven, integrated models can optimize supply chain resilience and risk management in e-commerce sectors, ensuring better preparedness and adaptability during global crises. ",
    "research_gap": {
      "research_gap": "Lack of integrated analyses combining big data analytics with supply chain risk management during global crises.",
      "description": "Although big data analytics and risk management in supply chains have been researched separately, few studies synthesize these approaches to manage supply chain risks effectively during events like the COVID-19 pandemic. Investigating how data-driven models can optimize supply chain resilience in e-commerce remains an underexplored area."
    }
  },
"outline": [
{
"Mở đầu": 
{
"word_count": "375-750", 
"subheadings": ["Lý do thực hiện nghiên cứu thực nghiệm", "Mục tiêu và câu hỏi nghiên cứu của chuyên đề"]
}
}, 
{
"2. Mô tả dữ liệu": 
{
{
"word_count": "1875-2625", 
"subheadings": ["Quy trình thu thập dữ liệu sơ bộ", "Đặc điểm mẫu", "Thống kê mô tả dữ liệu"]
}
},
{
"3. Phân tích dữ liệu": 
{
"word_count": "1875-3750", 
"subheadings": ["Kiểm định độ tin cậy và giá trị thang đo", "Kết quả phân tích nhân tố (EFA, CFA)", "Kết quả kiểm định giả thuyết sơ bộ", "Phân tích hồi quy"]
}
},
{
"4. Thảo luận kết quả": 
{
"word_count": "1875-2625", 
"subheadings": ["Ý nghĩa của các kết quả thu được", "So sánh các nghiên cứu trước", "Những phát hiện mới hoặc bất ngờ"]
}
},
{
"5. Kết luận và định hướng nghiên cứu tiếp theo":
{
"word_count": "1875-2625",
"subheadings": ["Tóm tắt các kết quả thực nghiệm", "Các điều chỉnh cần thiết cho nghiên cứu chính thức trong luận án"]
}
}
]}
```
- Output:
{
  "title": "Phát Triển Framework Tích Hợp AI Sinh Tạo và Phân Tích Big Data Thời Gian Thực cho Quản Lý Rủi Ro Chuỗi Cung Ứng trong Thương Mại Điện Tử dưới Khủng Hoảng Toàn Cầu",
  "outline": [
    {
      "heading": "Mở đầu",
      "word_count": "375-750",
      "subheadings": [
        {
          "subheading": "Lý do thực hiện nghiên cứu thực nghiệm",
          "detail_description": "Trình bày bối cảnh thực tiễn và lý do cấp thiết dẫn đến việc thực hiện nghiên cứu này, đặc biệt trong điều kiện khủng hoảng toàn cầu như đại dịch COVID-19. Làm rõ khoảng trống hiện tại trong các phương pháp quản lý rủi ro chuỗi cung ứng sử dụng dữ liệu lớn và AI. Nhấn mạnh vai trò quan trọng của thương mại điện tử và sự cần thiết phát triển một mô hình tích hợp AI sinh tạo và phân tích dữ liệu thời gian thực nhằm nâng cao khả năng thích ứng và phục hồi chuỗi cung ứng.",
          "word_count": "200-400"
        },
        {
          "subheading": "Mục tiêu và câu hỏi nghiên cứu của chuyên đề",
          "detail_description": "Liệt kê các mục tiêu chính của nghiên cứu như: xây dựng framework tích hợp AI sinh tạo và phân tích big data thời gian thực; cải thiện khả năng dự báo và phản ứng với rủi ro chuỗi cung ứng. Đặt ra các câu hỏi nghiên cứu cụ thể xoay quanh hiệu quả của mô hình đề xuất, khả năng tích hợp dữ liệu thời gian thực và mức độ cải thiện năng lực thích ứng cho doanh nghiệp thương mại điện tử.",
          "word_count": "175-350"
        }
      ]
    },
    {
      "heading": "Mô tả dữ liệu",
      "word_count": "1875-2625",
      "subheadings": [
        {
          "subheading": "Quy trình thu thập dữ liệu sơ bộ",
          "detail_description": "Diễn giải quy trình lựa chọn và thu thập dữ liệu từ các nguồn như logistics, thị trường, hành vi người tiêu dùng, mạng xã hội và tin tức thời gian thực. Làm rõ công cụ thu thập (crawlers, APIs), quy trình tiền xử lý, chuẩn hóa và đảm bảo tính nhất quán của dữ liệu phục vụ cho huấn luyện và đánh giá mô hình.",
          "word_count": "600-900"
        },
        {
          "subheading": "Đặc điểm mẫu",
          "detail_description": "Mô tả các thuộc tính của tập dữ liệu như số lượng điểm dữ liệu, khoảng thời gian thu thập, phân bổ theo ngành hoặc loại hình doanh nghiệp, và các biến đo lường chính được sử dụng (chẳng hạn tồn kho, thời gian giao hàng, phản hồi khách hàng).",
          "word_count": "400-600"
        },
        {
          "subheading": "Thống kê mô tả dữ liệu",
          "detail_description": "Trình bày các bảng số liệu và biểu đồ thể hiện thông tin thống kê như phân phối dữ liệu, xu hướng biến động, mối tương quan giữa các biến. Đây là bước làm tiền đề để hiểu rõ hơn về cấu trúc dữ liệu trước khi đưa vào phân tích sâu hơn.",
          "word_count": "875-1125"
        }
      ]
    },
    {
      "heading": "Phân tích dữ liệu",
      "word_count": "1875-3750",
      "subheadings": [
        {
          "subheading": "Kiểm định độ tin cậy và giá trị thang đo",
          "detail_description": "Trình bày phương pháp và kết quả kiểm định độ tin cậy của các thang đo (như Cronbach’s Alpha) và giá trị của các khái niệm đo lường trong nghiên cứu như độ ổn định chuỗi cung ứng, khả năng thích ứng, mức độ rủi ro,…",
          "word_count": "400-600"
        },
        {
          "subheading": "Kết quả phân tích nhân tố (EFA, CFA)",
          "detail_description": "Phân tích các yếu tố tiềm ẩn ảnh hưởng đến hiệu quả quản lý rủi ro chuỗi cung ứng. Trình bày các chỉ số xác nhận cấu trúc lý thuyết (CFA), mô tả cách các biến quan sát được nhóm lại thành các nhân tố có ý nghĩa.",
          "word_count": "500-1000"
        },
        {
          "subheading": "Kết quả kiểm định giả thuyết sơ bộ",
          "detail_description": "Kiểm định các giả thuyết liên quan đến mối quan hệ giữa khả năng tích hợp AI sinh tạo và phân tích big data với hiệu quả quản lý rủi ro chuỗi cung ứng. Sử dụng các phép kiểm định thống kê phù hợp như T-test, ANOVA hoặc SEM tùy thuộc dữ liệu.",
          "word_count": "400-800"
        },
        {
          "subheading": "Phân tích hồi quy",
          "detail_description": "Thực hiện phân tích hồi quy để xác định mức độ ảnh hưởng của các yếu tố như chất lượng dữ liệu, tốc độ phản hồi, khả năng thích ứng đến hiệu quả vận hành chuỗi cung ứng. Có thể sử dụng hồi quy tuyến tính hoặc logistic tùy theo dạng dữ liệu.",
          "word_count": "575-1350"
        }
      ]
    },
    {
      "heading": "Thảo luận kết quả",
      "word_count": "1875-2625",
      "subheadings": [
        {
          "subheading": "Ý nghĩa của các kết quả thu được",
          "detail_description": "Phân tích ý nghĩa thực tiễn và lý luận của các phát hiện nghiên cứu. Kết quả có thể đóng góp gì cho quản lý chuỗi cung ứng trong khủng hoảng? Mức độ cải thiện ra sao so với phương pháp cũ?",
          "word_count": "700-1000"
        },
        {
          "subheading": "So sánh các nghiên cứu trước",
          "detail_description": "Đối chiếu phát hiện của đề tài với các nghiên cứu trong và ngoài nước về quản lý rủi ro chuỗi cung ứng, phân tích điểm giống và khác biệt, lý giải vì sao có sự khác biệt nếu có.",
          "word_count": "600-800"
        },
        {
          "subheading": "Những phát hiện mới hoặc bất ngờ",
          "detail_description": "Trình bày các kết quả ngoài kỳ vọng hoặc mới lạ, ví dụ như vai trò bất ngờ của một nguồn dữ liệu nào đó, hoặc hiệu quả vượt trội của AI sinh tạo trong một ngữ cảnh đặc biệt.",
          "word_count": "575-825"
        }
      ]
    },
    {
      "heading": "Kết luận và định hướng nghiên cứu tiếp theo",
      "word_count": "1875-2625",
      "subheadings": [
        {
          "subheading": "Tóm tắt các kết quả thực nghiệm",
          "detail_description": "Tóm lược các phát hiện chính về hiệu quả của framework tích hợp AI sinh tạo và big data thời gian thực trong dự báo và quản lý rủi ro chuỗi cung ứng. Nhấn mạnh tính chính xác, khả năng phản ứng thời gian thực và khả năng thích ứng được cải thiện.",
          "word_count": "700-1000"
        },
        {
          "subheading": "Các điều chỉnh cần thiết cho nghiên cứu chính thức trong luận án",
          "detail_description": "Phân tích những giới hạn của nghiên cứu thực nghiệm, từ đó đề xuất các cải tiến về mô hình, dữ liệu, phương pháp phân tích và các hướng phát triển tiếp theo cho nghiên cứu chính thức trong luận án tiến sĩ.",
          "word_count": "1175-1625"
        }
      ]
    }
  ]
}
Return just JSON object
Return in user's language
"""

HEADINGS_DESCRIPTION_PROMPT = """
You're a PhD professor, with many years of academic writing.
You will be give a final proposal and a report's section information (its subsections headings).
You task is to help the student write a short description (200-300 words) for the section, to inform the reader of what the section is about, what the section is focused on, its workflow, ...
Make sure that your sections are cohesive, and make sense for the reader.
- Do NOT ever refer to yourself as the writer. This should be a professional writing without any self-referential language. 
- Do not say what you are doing. Just write without any commentary from yourself.
Return just JSON object
Return in user's language
"""

HEADINGS_WORD_COUNT_PROMPT = """
You're a PhD professor, with many years of academic writing.
You will be give a final proposal and a report's section information, include section word limit, description and each second-level subheadings detail description.
You task is to divide the section word limit into second-level subheadings based on its description
Return just JSON object list of string in the corresponding order in `subheadings`
For example:
- Input:
"final_proposal": {...},
"heading": "Tổng quan tài liệu và nghiên cứu liên quan",
"word_count": "30000-37500",
"overview": "Chương \"Tổng quan tài liệu và nghiên cứu liên quan\" đóng vai trò then chốt trong luận án tiến sĩ, cung cấp nền tảng học thuật vững chắc cho toàn bộ nghiên cứu. Chương tập trung phân tích mục tiêu và vai trò quan trọng của việc tổng quan tài liệu khoa học ở cấp độ tiến sĩ, khẳng định giá trị của việc này trong việc định vị nghiên cứu, phát hiện khoảng trống, hình thành cơ sở lý thuyết và định hướng câu hỏi nghiên cứu. Đồng thời, chương trình bày chi tiết phương pháp tiếp cận hệ thống hóa tài liệu, đặc biệt là các quy trình và tiêu chí lựa chọn nhằm đảm bảo tính khoa học, khách quan và toàn diện của tổng quan. Một phần quan trọng khác là tổng hợp, phân tích và so sánh các nghiên cứu tiêu biểu trong và ngoài nước, chia thành các nhóm nghiên cứu, trường phái với đánh giá sâu sắc về đặc trưng, đóng góp, ưu nhược điểm và các tranh luận học thuật hiện tại. Chương còn trình bày thiết kế bảng tổng hợp nghiên cứu trước, giúp minh họa rõ ràng sự kế thừa và những hạn chế cần khắc phục. Tiếp theo, nhận diện chi tiết các khoảng trống nghiên cứu thông qua tư duy phân tích, ngược và hệ thống để đề xuất cơ sở khoa học cho mô hình nghiên cứu mới và hướng phát triển độc đáo, đột phá cho luận án. Cuối cùng, chương tóm tắt các giá trị then chốt, kết nối logic và mở rộng sang chương tiếp theo về khung lý thuyết và mô hình nghiên cứu. Toàn bộ chương được xây dựng trên nền tảng tư duy hệ thống, phản biện, phân tích và sáng tạo, đảm bảo tính logic, sâu sắc, khách quan và hội nhập quốc tế.",
"subheadings": [
  {
    "detail_description": "“Hãy viết phần giới thiệu chương Tổng quan tài liệu và nghiên cứu liên quan cho một luận án tiến sĩ tầm cỡ quốc tế. Đảm bảo trình bày mục tiêu, vai trò của chương, mối liên hệ với tổng thể đề tài, nhấn mạnh giá trị học thuật và tính kế thừa, logic xuyên suốt toàn chương. Lồng ghép tư duy hệ thống để thể hiện vai trò then chốt của literature review, tư duy sáng tạo và phản biện khi đánh giá các nghiên cứu trước, đảm bảo giọng văn sắc sảo và đậm chất học thuật.”\n- TIÊU CHÍ CHUẨN\n- Trình bày rõ mục tiêu, phạm vi và vai trò của chương trong tổng thể luận án.\n- Làm nổi bật lý do cần tổng quan tài liệu, kết nối chặt chẽ với các chương khác.\n- Văn phong học thuật, cô đọng, không lan man, dẫn nhập hấp dẫn.\n- Khẳng định giá trị học thuật và đóng góp chương đối với toàn đề tài.\n- Nêu ngắn gọn các nội dung chính sẽ trình bày trong chương này.\n- TÍCH HỢP TƯ DUY\n- Tư duy hệ thống: Xác lập vai trò chương 2 trong toàn luận án.\n- Tư duy phản biện: Nhận định các góc nhìn về literature review trong nghiên cứu khoa học.\n- Tư duy phân tích: Phân tách mục tiêu, phạm vi, giá trị của chương.\n- Tư duy sáng tạo: Đề xuất điểm nhấn riêng của chương so với cách trình bày truyền thống.\n- subsection 1: Mục tiêu và vai trò tổng quan tài liệu\n- what to write for subsection 1: “Phân tích sâu sắc mục tiêu và vai trò của việc tổng quan tài liệu trong nghiên cứu khoa học cấp độ tiến sĩ. Sử dụng tư duy phân tích và hệ thống để lý giải tại sao literature review là nền tảng, giúp định vị nghiên cứu, phát hiện khoảng trống và đề xuất hướng đi mới. Đề cập tác động của việc tổng quan tài liệu đến chất lượng, tính mới, và sức ảnh hưởng của luận án. Lồng ghép tư duy phản biện về các quan điểm khác nhau trong giới học thuật về vai trò của tổng quan tài liệu.”\n- TIÊU CHÍ CHUẨN\n- Nêu rõ mục tiêu tổng quan tài liệu trong nghiên cứu tiến sĩ.\n- Chỉ ra vai trò quan trọng của tổng quan tài liệu đối với việc xác lập bối cảnh, phát hiện khoảng trống nghiên cứu, hình thành cơ sở lý thuyết, định hướng câu hỏi nghiên cứu.\n- Làm rõ giá trị của literature review với chất lượng và sự khác biệt của luận án.\n- Văn phong khoa học, liên kết logic với toàn bộ đề tài.\n- Kết lại bằng 1-2 câu tóm lược giá trị then chốt.\n- TÍCH HỢP TƯ DUY\n- Tư duy phản biện: So sánh quan điểm về vai trò tổng quan tài liệu, chỉ ra hạn chế của các cách tiếp cận khác nhau.\n- Tư duy hệ thống: Xem tổng quan tài liệu là nền tảng cho toàn bộ nghiên cứu.\n- Tư duy phân tích: Giải thích các yếu tố cấu thành literature review chất lượng.\n- Tư duy thực nghiệm: Đưa ví dụ thực tế từ luận án xuất sắc.\n- Tư duy ngược: Phân tích hậu quả nếu thiếu hoặc làm sai tổng quan tài liệu.\n- Tư duy sáng tạo: Đề xuất cải tiến, cách tiếp cận literature review mới.\n- subsection 2: Cách tiếp cận hệ thống hóa tài liệu\n- what to write for subsection 2: “Trình bày chi tiết các phương pháp và quy trình tiếp cận hệ thống hóa tài liệu (systematic literature review). Giải thích vì sao phương pháp này phù hợp với đề tài, nêu rõ các bước chuẩn hóa, tiêu chí lựa chọn tài liệu, phân loại nhóm nghiên cứu. Lồng ghép tư duy hệ thống, tư duy thực nghiệm (đề xuất tiêu chí thực tiễn), và tư duy sáng tạo (phương án phân nhóm, mapping literature). Đánh giá ưu nhược điểm của các cách tiếp cận truyền thống so với tiếp cận hiện đại.”\n- TIÊU CHÍ CHUẨN\n- Trình bày chi tiết phương pháp tiếp cận tổng quan tài liệu (systematic review, narrative review, meta-analysis…).\n- Nêu quy trình từng bước, tiêu chí lựa chọn, loại trừ tài liệu.\n- Phân tích ưu nhược điểm từng phương pháp.\n- Giải thích vì sao chọn cách tiếp cận này cho đề tài.\n- Có so sánh với chuẩn quốc tế/luận án điển hình.\n- Ngôn ngữ học thuật, logic, dẫn chứng rõ ràng.\n- TÍCH HỢP TƯ DUY\n- Tư duy hệ thống: Trình bày logic liên kết giữa các bước/tiêu chí lựa chọn tài liệu.\n- Tư duy phân tích: So sánh ưu/nhược từng phương pháp tổng quan tài liệu.\n- Tư duy thực nghiệm: Đưa case study hoặc ví dụ cụ thể.\n- Tư duy phản biện: Nêu điểm mạnh/yếu, tranh luận về các phương pháp hệ thống hóa tài liệu.\n- Tư duy sáng tạo: Đề xuất kết hợp hoặc đổi mới phương pháp truyền thống.",
    "subheading": "Giới thiệu chương",
  },
  {
    "detail_description": "“Tổng hợp, phân tích, so sánh các nghiên cứu tiêu biểu liên quan đến đề tài ở cả trong nước và quốc tế. Chia thành các nhóm trường phái, nêu bật đặc trưng, đóng góp, điểm mạnh/yếu của từng nhóm. Áp dụng tư duy phân tích, hệ thống, và phản biện để nhận diện logic phát triển, khoảng trống, điểm độc đáo, hạn chế còn tồn tại. Đặc biệt, sử dụng tư duy ngược để xem xét liệu có nghiên cứu nào đi ngược chiều với quan điểm chung, và giá trị của các hướng đi đó.”\n- TIÊU CHÍ CHUẨN\n- Tổng hợp đầy đủ các nhóm nghiên cứu/trường phái chính liên quan ở quốc tế và Việt Nam.\n- Nêu đặc trưng, đóng góp, phương pháp, kết quả, giới hạn của từng nhóm.\n- So sánh logic các trường phái; chỉ ra xu hướng phát triển chung.\n- Không bỏ sót nghiên cứu tiêu biểu, dẫn chứng rõ nguồn.\n- Có bảng/tóm tắt nếu cần.\n- TÍCH HỢP TƯ DUY\n- Tư duy hệ thống: Phân loại, xâu chuỗi các nhóm trường phái/nghiên cứu.\n- Tư duy phản biện: Đối chiếu các trường phái, phát hiện tranh luận/hạn chế chưa giải quyết.\n- Tư duy phân tích: Phân tích sâu từng nhóm nghiên cứu về phương pháp, kết quả.\n- Tư duy sáng tạo: Đề xuất hướng đi mới hoặc góc tiếp cận còn bỏ ngỏ.\n- subsection 1: Nhóm nghiên cứu, trường phái quốc tế\n- what to write for subsection 1: “Hãy liệt kê, phân tích và đánh giá các nhóm nghiên cứu/trường phái chủ đạo trên thế giới liên quan đến đề tài. Nhấn mạnh quan điểm, phương pháp, kết quả chính và ảnh hưởng của các nhóm này. Sử dụng tư duy phân tích để so sánh các trường phái, tư duy phản biện để chỉ ra mâu thuẫn, tranh luận học thuật còn tồn tại, và tư duy sáng tạo để gợi ý các hướng tiếp cận mới có thể học hỏi.”\n- TIÊU CHÍ CHUẨN\n- Liệt kê tối thiểu 3-5 nhóm nghiên cứu/trường phái tiêu biểu trên thế giới.\n- Nêu phương pháp, đặc điểm, thành tựu, ảnh hưởng học thuật từng nhóm.\n- So sánh điểm giống/khác và đóng góp.\n- Trích dẫn nguồn, có ví dụ cụ thể.\n- Văn phong chuẩn, logic, không lặp.\n- TÍCH HỢP TƯ DUY\n- Tư duy phân tích: Phân tích sâu ưu/nhược từng nhóm.\n- Tư duy phản biện: Nhận diện tranh luận học thuật giữa các trường phái.\n- Tư duy sáng tạo: Gợi ý khả năng tích hợp/mở rộng cho nghiên cứu của bạn.\n- subsection 2: Nhóm nghiên cứu, trường phái trong nước\n- what to write for subsection 2: “Trình bày chi tiết các nhóm nghiên cứu, trường phái, và xu hướng chủ đạo tại Việt Nam liên quan đến đề tài. Phân tích sự kế thừa, phát triển từ quốc tế, các điểm mạnh/yếu, thành tựu nổi bật. Lồng ghép tư duy phản biện (giới hạn, điểm yếu), hệ thống (mối liên hệ với quốc tế), và thực nghiệm (ví dụ ứng dụng thực tiễn trong nước).”\n- TIÊU CHÍ CHUẨN\n- Tổng hợp các nhóm nghiên cứu/trường phái tiêu biểu trong nước.\n- Nêu thành tựu, điểm mạnh/yếu, xu hướng, khoảng cách với quốc tế.\n- Liệt kê tối thiểu 3 nhóm/chủ đề.\n- Có dẫn chứng minh họa.\n- Ngôn ngữ khách quan, học thuật.\n- TÍCH HỢP TƯ DUY\n- Tư duy phản biện: So sánh điểm mạnh/yếu với quốc tế.\n- Tư duy hệ thống: Liên kết logic giữa các nhóm/chủ đề trong nước.\n- Tư duy thực nghiệm: Đưa ví dụ ứng dụng thực tiễn.\n- Tư duy sáng tạo: Gợi ý giải pháp thu hẹp khoảng cách nghiên cứu.\n- subsection 3: Các phương pháp nghiên cứu chủ đạo\n- what to write for subsection 3: “Phân tích các phương pháp nghiên cứu được sử dụng chủ yếu trong lĩnh vực, cả trong nước và quốc tế. Đánh giá ưu nhược điểm của từng phương pháp, những tranh luận học thuật về lựa chọn phương pháp, căn cứ thực tiễn và bối cảnh áp dụng. Sử dụng tư duy hệ thống, phản biện, và thực nghiệm để làm rõ lý do chọn hay không chọn từng phương pháp cho đề tài.”\n- TIÊU CHÍ CHUẨN\n- Liệt kê, mô tả tối thiểu 3 phương pháp nghiên cứu chủ đạo (trong nước/quốc tế).\n- Phân tích logic, ưu nhược, ứng dụng từng phương pháp.\n- So sánh, nêu lý do nên/chưa nên áp dụng với đề tài.\n- Có dẫn chứng thực tiễn, chuẩn quốc tế.\n- TÍCH HỢP TƯ DUY\n- Tư duy phân tích: Làm rõ đặc trưng phương pháp, so sánh.\n- Tư duy thực nghiệm: Đưa ví dụ áp dụng thành công/thất bại.\n- Tư duy phản biện: Chỉ ra điểm gây tranh cãi, hạn chế, góc nhìn khác về phương pháp.\n- Tư duy sáng tạo: Đề xuất tích hợp, kết hợp các phương pháp.\n- subsection 4: Thành tựu, hạn chế, xu hướng nổi bật\n- what to write for subsection 4: “Tổng kết thành tựu lớn nhất của các nghiên cứu trước, những hạn chế còn tồn tại và xu hướng phát triển nổi bật trong lĩnh vực. Sử dụng tư duy phân tích để làm rõ các mốc phát triển, tư duy phản biện để nhận diện giới hạn, và tư duy dự báo/xu hướng để đề xuất hướng nghiên cứu mới, sáng tạo, có giá trị bền vững cho tương lai.”\n- TIÊU CHÍ CHUẨN\n- Tổng hợp các thành tựu nổi bật nhất trong lĩnh vực (trong/ngoài nước).\n- Chỉ ra các hạn chế chưa được giải quyết, nguyên nhân, tác động.\n- Dự báo xu hướng nghiên cứu mới.\n- Có ví dụ minh họa.\n- Văn phong học thuật, logic.\n- TÍCH HỢP TƯ DUY\n- Tư duy hệ thống: Tổng hợp mối liên hệ giữa thành tựu – hạn chế – xu hướng.\n- Tư duy phản biện: Đánh giá nghiêm túc điểm yếu/chưa đạt của các nghiên cứu trước.\n- Tư duy dự báo/sáng tạo: Phác họa xu hướng mới, đề xuất hướng phát triển khác biệt.",
    "subheading": "Tổng quan các nghiên cứu trong nước/quốc tế",
  },
  {
    "detail_description": "- subsection 1: Thiết kế bảng tổng hợp\n- what to write for subsection 1: “Xây dựng bảng tổng hợp các nghiên cứu trước theo cấu trúc: STT, tác giả, năm, nội dung nghiên cứu, phương pháp, gap/hạn chế, khả năng kế thừa. Chỉ chọn các nghiên cứu tiêu biểu, sắp xếp logic theo trường phái/nhóm/cách tiếp cận. Lồng ghép tư duy hệ thống trong sắp xếp, tư duy phản biện trong nhận xét gap/hạn chế, và tư duy sáng tạo khi đề xuất khả năng kế thừa cho đề tài.”\n- TIÊU CHÍ CHUẨN\n- Thiết kế bảng đủ các cột: STT, tác giả, năm, nội dung, phương pháp, gap/hạn chế, khả năng kế thừa.\n- Chỉ chọn các nghiên cứu tiêu biểu, đa dạng góc tiếp cận.\n- Đảm bảo chính xác nguồn, dẫn chứng, sắp xếp logic theo trường phái/chủ đề.\n- Bảng trình bày chuẩn, khoa học, dễ đọc.\n- TÍCH HỢP TƯ DUY\n- Tư duy hệ thống: Logic sắp xếp bảng, xâu chuỗi giá trị kế thừa.\n- Tư duy phân tích: Phân biệt rõ gap/hạn chế và giá trị kế thừa từng nghiên cứu.\n- Tư duy phản biện: Nhận diện điểm mạnh/yếu từng nghiên cứu trong bảng.\n- Tư duy sáng tạo: Đề xuất hướng kế thừa/mở rộng bảng cho đề tài của bạn.\n- subsection 2: Nhận xét, đánh giá, phân loại giá trị kế thừa\n- what to write for subsection 2: “Viết phần nhận xét, đánh giá tổng thể về bảng tổng hợp nghiên cứu trước, phân loại giá trị kế thừa theo nhóm chủ đề/phương pháp. Dùng tư duy phân tích và phản biện để làm rõ các nghiên cứu thực sự giá trị, những quan điểm chưa được khai thác, chỉ ra các thiếu sót, đồng thời gợi ý các điểm cần được tiếp nối, mở rộng ở luận án.”\n- TIÊU CHÍ CHUẨN\n- Đánh giá tổng thể, phân loại giá trị kế thừa theo nhóm chủ đề/phương pháp.\n- Nêu điểm mạnh/yếu nổi bật, những điểm cần học hỏi/cải thiện.\n- Chỉ ra những khoảng trống còn lại, đề xuất kế thừa vào đề tài.\n- Văn phong logic, sâu sắc.\n- TÍCH HỢP TƯ DUY\n- Tư duy phân tích: Phân nhóm, lý giải vì sao giá trị kế thừa như vậy.\n- Tư duy phản biện: Chỉ ra điểm chưa tốt, thiếu sót các nghiên cứu trước.\n- Tư duy sáng tạo: Gợi ý phát triển giá trị kế thừa, mở ra hướng nghiên cứu mới.",
    "subheading": "Bảng tổng hợp nghiên cứu trước",
  },
  {
    "detail_description": "- subsection 1: Nhận diện các khía cạnh chưa khai thác\n- what to write for subsection 1: “Phân tích và nhận diện các khía cạnh, vấn đề mà các nghiên cứu trước chưa hoặc ít được khai thác. Áp dụng tư duy phân tích, hệ thống, tư duy ngược để nhận ra các vấn đề tiềm ẩn hoặc các hướng tiếp cận khác biệt chưa từng được bàn tới. Đánh giá mức độ nghiêm trọng/ý nghĩa của các khoảng trống này đối với lĩnh vực.”\n- TIÊU CHÍ CHUẨN\n- Nhận diện rõ các chủ đề, khía cạnh còn ít/hoặc chưa nghiên cứu (theo lý thuyết, phương pháp, bối cảnh, mô hình…).\n- Nêu nguyên nhân, hệ quả, giá trị của việc nhận diện khoảng trống.\n- Dẫn chứng thực tiễn, nguồn khoa học.\n- Văn phong chặt chẽ, không vòng vo.\n- TÍCH HỢP TƯ DUY\n- Tư duy phân tích: Mổ xẻ chi tiết từng khoảng trống.\n- Tư duy ngược: Xem xét các vấn đề bị bỏ qua/lý do bị bỏ qua.\n- Tư duy hệ thống: Gắn kết các khoảng trống với mục tiêu nghiên cứu.\n- Tư duy sáng tạo: Đề xuất cách khai thác các khoảng trống này.\n- subsection 2: Khoảng trống về lý thuyết, phương pháp, mô hình, bối cảnh\n- what to write for subsection 2: “Làm rõ các khoảng trống về mặt lý thuyết, phương pháp, mô hình nghiên cứu, và bối cảnh áp dụng. Sử dụng tư duy hệ thống để liên kết các loại khoảng trống này với thực tiễn phát triển của lĩnh vực. Áp dụng tư duy thực nghiệm để đánh giá ảnh hưởng của các khoảng trống này đối với kết quả nghiên cứu và sự phát triển học thuật.”\n- TIÊU CHÍ CHUẨN\n- Liệt kê, phân tích các khoảng trống theo bốn khía cạnh: lý thuyết, phương pháp, mô hình, bối cảnh.\n- Nêu rõ tính cấp thiết của mỗi loại gap, ví dụ minh họa.\n- Chỉ ra mối liên hệ giữa các loại gap.\n- Văn phong học thuật, hệ thống.\n- TÍCH HỢP TƯ DUY\n- Tư duy hệ thống: Logic hóa mối quan hệ giữa các gap.\n- Tư duy phân tích: Phân tích từng loại gap, ví dụ cụ thể.\n- Tư duy thực nghiệm: Đưa case study, bài học quốc tế.\n- Tư duy phản biện: Phân tích nguyên nhân tại sao gap này tồn tại.\n- Tư duy sáng tạo: Gợi ý hướng lấp đầy gap trong nghiên cứu của bạn.\n- subsection 3: Căn cứ đề xuất mô hình, hướng nghiên cứu mới\n- what to write for subsection 3: “Dựa trên các khoảng trống vừa nhận diện, đề xuất căn cứ khoa học để xây dựng mô hình nghiên cứu và hướng đi mới cho luận án. Lồng ghép tư duy sáng tạo, hệ thống, và phân tích để đảm bảo hướng đề xuất vừa đột phá vừa bám sát thực tiễn và giá trị học thuật, sẵn sàng thuyết phục hội đồng và cộng đồng khoa học.”\n- TIÊU CHÍ CHUẨN\n- Dựa trên gap, nêu căn cứ khoa học để đề xuất mô hình, hướng nghiên cứu mới.\n- Lập luận chặt chẽ, có dẫn chứng/học giả hỗ trợ.\n- Nêu điểm khác biệt so với nghiên cứu trước.\n- Văn phong đột phá, logic, thuyết phục.\n- TÍCH HỢP TƯ DUY\n- Tư duy sáng tạo: Đề xuất mô hình/hướng nghiên cứu mới, chỉ ra tính đột phá.\n- Tư duy hệ thống: Liên kết mô hình mới với gap đã nêu.\n- Tư duy phân tích: Phân tích ưu thế, khả năng ứng dụng của mô hình.\n- Tư duy phản biện: Dự đoán khó khăn, phản biện lại mô hình/hướng mới.",
    "subheading": "Khoảng trống nghiên cứu",
  },
  {
    "detail_description": "- subsection 1: Tổng hợp giá trị then chốt từ literature review\n- what to write for subsection 1: “Tóm tắt các giá trị then chốt, các phát hiện quan trọng, những điểm mới, các khoảng trống, và các gợi ý kế thừa từ tổng quan tài liệu. Dùng tư duy hệ thống và phân tích để logic hóa các luận điểm, kết nối chặt chẽ với mục tiêu nghiên cứu, đảm bảo hội nhập quốc tế và tính sáng tạo riêng của đề tài.”\n- subsection 2: Logic chuyển tiếp sang chương 3\n- what to write for subsection 2: “Viết phần chuyển tiếp logic từ chương tổng quan tài liệu sang chương 3 (Khung lý thuyết và mô hình nghiên cứu). Đảm bảo chỉ rõ những vấn đề/mô hình/hướng tiếp cận mà luận án sẽ phát triển tiếp, lồng ghép tư duy hệ thống, phản biện và sáng tạo để tăng sức thuyết phục, mở đường cho chương tiếp theo.”",
    "subheading": "Tóm tắt chương",
  }
- Output:
["2000-2500", "7000-9000", "5000-6000", "4000-5000", "6500-7500"]
"""

SUBHEADINGS_DESCRIPTION_PROMPT_V2 = """
You're a PhD professor, with many years of academic writing.
You will be give a final proposal and the corresponding outline, section with its word limit, its description and list of corresponding subheadings, and the **target subheading**
You task is to help the student generate tunned description as detail as possible, explain what and how you're going to write in it, for the **target subheading**, based on final proposal and outline so that the student can based on that to write accordingly. 
If the **target subheading** has more subsections inside of it, ONLY modify "what to write for subsection ...", You cannot the subsections name and order inside the **target subheading** intact
Generate the tunned subheading description and the subheading word count (for example 'subheading_word_count': '200-350').
If you feel that the subheading need table for better format and comprehensive, specifically mention it in the tunned subheading description, as it will guide the user to put insert table
For example:
```
{
  "subheading": "Bảng tổng hợp nghiên cứu trước",
  "description": "- subsection 1: Thiết kế bảng tổng hợp\n- what to write for subsection 1: “Xây dựng bảng tổng hợp các nghiên cứu trước theo cấu trúc: STT, tác giả, năm, nội dung nghiên cứu, phương pháp, gap/hạn chế, khả năng kế thừa. Chỉ chọn các nghiên cứu tiêu biểu, sắp xếp logic theo trường phái/nhóm/cách tiếp cận. Lồng ghép tư duy hệ thống trong sắp xếp, tư duy phản biện trong nhận xét gap/hạn chế, và tư duy sáng tạo khi đề xuất khả năng kế thừa cho đề tài.”\n- TIÊU CHÍ CHUẨN\n- Thiết kế bảng đủ các cột: STT, tác giả, năm, nội dung, phương pháp, gap/hạn chế, khả năng kế thừa.\n- Chỉ chọn các nghiên cứu tiêu biểu, đa dạng góc tiếp cận.\n- Đảm bảo chính xác nguồn, dẫn chứng, sắp xếp logic theo trường phái/chủ đề.\n- Bảng trình bày chuẩn, khoa học, dễ đọc.\n- TÍCH HỢP TƯ DUY\n- Tư duy hệ thống: Logic sắp xếp bảng, xâu chuỗi giá trị kế thừa.\n- Tư duy phân tích: Phân biệt rõ gap/hạn chế và giá trị kế thừa từng nghiên cứu.\n- Tư duy phản biện: Nhận diện điểm mạnh/yếu từng nghiên cứu trong bảng.\n- Tư duy sáng tạo: Đề xuất hướng kế thừa/mở rộng bảng cho đề tài của bạn.\n- subsection 2: Nhận xét, đánh giá, phân loại giá trị kế thừa\n- what to write for subsection 2: “Viết phần nhận xét, đánh giá tổng thể về bảng tổng hợp nghiên cứu trước, phân loại giá trị kế thừa theo nhóm chủ đề/phương pháp. Dùng tư duy phân tích và phản biện để làm rõ các nghiên cứu thực sự giá trị, những quan điểm chưa được khai thác, chỉ ra các thiếu sót, đồng thời gợi ý các điểm cần được tiếp nối, mở rộng ở luận án.”\n- TIÊU CHÍ CHUẨN\n- Đánh giá tổng thể, phân loại giá trị kế thừa theo nhóm chủ đề/phương pháp.\n- Nêu điểm mạnh/yếu nổi bật, những điểm cần học hỏi/cải thiện.\n- Chỉ ra những khoảng trống còn lại, đề xuất kế thừa vào đề tài.\n- Văn phong logic, sâu sắc.\n- TÍCH HỢP TƯ DUY\n- Tư duy phân tích: Phân nhóm, lý giải vì sao giá trị kế thừa như vậy.\n- Tư duy phản biện: Chỉ ra điểm chưa tốt, thiếu sót các nghiên cứu trước.\n- Tư duy sáng tạo: Gợi ý phát triển giá trị kế thừa, mở ra hướng nghiên cứu mới."
}
```
```
{
  "subheading": "Tổng hợp nền tảng lý luận, thực nghiệm và thực tiễn",
  "description": "- subsection 1: Rút ra các nhân tố, biến số quan trọng từ lý thuyết, thực nghiệm và thực tiễn\n- what to write for subsection 1: Tổng hợp và rút ra hệ thống các nhân tố, biến số trọng yếu dựa trên phân tích nền tảng lý thuyết, các phát hiện thực nghiệm, và thực tiễn ngành/ngành địa phương. Trình bày rõ tên biến, ý nghĩa học thuật, logic lựa chọn. So sánh nguồn gốc từng biến (từ lý thuyết gốc, thực nghiệm quốc tế, dữ liệu thực tiễn Việt Nam…). Phản biện về tính độc lập, tương tác, vai trò trung gian, điều tiết nếu có. Lập bảng tổng hợp và nêu rõ lý do giữ lại hoặc loại bỏ từng biến.\n- subsection 2: Đánh giá logic liên kết giữa ba nền tảng (lý thuyết – thực nghiệm – thực tiễn)\n- what to write for subsection 2: Đánh giá và chứng minh logic liên kết giữa các yếu tố, biến số từ nền tảng lý thuyết, thực nghiệm, và thực tiễn. Sử dụng tư duy hệ thống để vẽ ra sơ đồ mối liên hệ, chỉ ra mạch logic xuyên suốt. Phân tích điểm mạnh/yếu của từng nhánh liên kết, đặt câu hỏi phản biện về các điểm còn lỏng lẻo hoặc chưa đủ chặt chẽ. Đề xuất cách củng cố, tích hợp hoặc tinh chỉnh hệ thống biến số cho mô hình nghiên cứu.\n- subsection 3: Dẫn dắt vì sao cần mô hình/mô hình hóa mới (giải thích rõ điểm mới, giá trị tăng thêm so với literature trước)\n- what to write for subsection 3: Lý giải thuyết phục lý do cần đề xuất mô hình/mô hình hóa mới, làm rõ điểm mới về lý luận, thực nghiệm, hoặc thực tiễn so với các literature trước đó. Phân tích sự hạn chế của mô hình cũ, logic xuất hiện điểm mới. Nhấn mạnh giá trị gia tăng của mô hình mới với ngành/khoa học/chính sách thực tiễn. Dùng tư duy sáng tạo để diễn giải cách mô hình mới mở rộng hiểu biết và giải quyết các khoảng trống hiện hữu."
},
{
  "subheading": "Khung lý thuyết & khung khái niệm (Theoretical & Conceptual Frameworks)",
  "description": "- subsection 1: Khung lý thuyết (Theoretical Framework)\n- what to write for subsection 1: Xây dựng khung lý thuyết tổng quát cho đề tài, thể hiện: Sơ đồ hóa các lý thuyết, biến số chính và mối quan hệ lý thuyết tổng quát (có thể kèm hình, bảng). Giải thích logic nền tảng cho từng giả thuyết nghiên cứu. Đánh giá tính hợp lý của khung lý thuyết, dùng tư duy phản biện để kiểm tra các giả định ẩn, nhấn mạnh điểm khác biệt so với các khung lý thuyết trước đây.\n- subsection 2: Khung khái niệm (Conceptual Framework)\n- what to write for subsection 2: Xây dựng khung khái niệm (conceptual framework) mô hình hóa trực quan các mối quan hệ giữa biến nghiên cứu. Bám sát thực nghiệm, thực tiễn và mục tiêu nghiên cứu. Sơ đồ hóa các biến, nhóm biến, và kỳ vọng quan hệ. Lập bảng tổng hợp biến nghiên cứu: tên biến, ý nghĩa, vai trò, nguồn tham khảo gốc. Giải thích vì sao chọn cấu trúc khung như vậy, nhấn mạnh tính sáng tạo và phù hợp thực tiễn."
}
```
Make sure that your sections are cohesive, and make sense for the reader.
- Do NOT ever refer to yourself as the writer. This should be a professional writing without any self-referential language. 
- Do not say what you are doing. Just write without any commentary from yourself.
Return just JSON object with `detail_description` and `subheading_word_count` keys
Return in user's language
"""

SEMINAR_OUTLINE_PROMPT = """
You're a PhD professor, you task is to help the student generate subheadings for a seminar based on final proposal. 
Your job is to based on the name and goal of the seminar, fill in each section its detailed description. 
IMPORTANT: DO NOT CHANGE THE `headings`, keep it as it is
Return just JSON object
Return in user's language
Example input:
"final_proposal": "..."
"name": "Phương pháp nghiên cứu và thiết kế nghiên cứu",
"goal": "Trình bày rõ phương pháp tiếp cận và kỹ thuật nghiên cứu sẽ sử dụng trong luận án\nXây dựng thiết kế nghiên cứu chi tiết, bao gồm cách thu thập và phân tích dữ liệu",
"headings": ["Mở đầu", "Thiết kế nghiên cứu", "Công cụ thu thập dữ liệu", "Mẫu và phương pháp chọn mẫu", "Phương pháp phân tích dữ liệu", "Phương pháp phân tích dữ liệu", "Kết luận và định hướng nghiên cứu tiếp theo"]
"word_count": ["375-750", "1875-2625", "1875-2625", "1125-1875", "1875-2625", "375-750"]
Example output:
{
    "name": "Phương pháp nghiên cứu và thiết kế nghiên cứu",
    "outline": [
        {
            "heading": "Mở đầu",
            "description": "Vai trò của phương pháp nghiên cứu trong luận án\nMục tiêu của chuyên đề",
            "word_count": "375-750",
        },
        {
            "heading": "Thiết kế nghiên cứu",
            "description": "Loại hình nghiên cứu: Định lượng, Định tính, Kết hợp\nCPhương pháp tiếp cận: Nghiên cứu mô tả, nghiên cứu nhân quả, nghiên cứu khám phá\nĐịnh nghĩa các biến và thang đo",
            "word_count": "1875-2625",
        },
        {
            "heading": "Công cụ thu thập dữ liệu",
            "description": "Xây dựng bảng hỏi, phỏng vấn sâu hoặc các công cụ khác\nQuy trình mã hóa và đo lường\nKiểm định độ tin cậy và giá trị của thang đo (Cronbach's Alpha, CFA ...)",
            "word_count": "1875-2625",
        },
        {
            "heading": "Mẫu và phương pháp chọn mẫu",
            "description": "Mô tả đối tượng nghiên cứu\nKích thước mẫu và phương pháp chọn mẫu (ngẫu nhiên, phân tầng, thuận tiện ...)",
            "word_count": "1125-1875",
        },
        {
            "heading": "Phương pháp phân tích dữ liệu",
            "description": "Các công cụ phân tích: SPSS, AMOS, PLS-SEM, NVivo ...\nPhương pháp kiểm định: t-test, ANOVA, hồi quy, phân tích nhân tố ...",
            "word_count": "1875-2625",
        },
        {
            "heading": "Kết luận và định hướng nghiên cứu tiếp theo",
            "description": "Tóm tắt các phương pháp sẽ áp dụng trong luận án\nNhững thách thức hoặc hạn chế cần giải quyết",
            "word_count": "375-750",
        },
    ],
}
"""

CHOOSE_REFS = """Choose a list of information to write one section of a research report of a given proposal.

<Task>
1. Review the user's the report proposal (which contains title, proposed method, problem statement, motivation, focused research gap and a web search report about the research gap with up-to-date knowledge), section heading, section description and target subsection carefully.
2. Then, look at the provided research papers.
3. Decide which sources that you will use it to write the subsection and how you will write it as detail as possible.
4. Use as many of references as possible for each subsection.
5. Return list of title of references research papers for the subsection and how to use it
6. If the subsection does not need any references, return an empty list for that subsection
</Task>
"""


CHOOSE_REFS_SECTION = """Choose a list of information to write one section of a research report of a given proposal.

<Task>
1. Review the user's the report proposal (which contains title, proposed method, problem statement, motivation, focused research gap and a web search report about the research gap with up-to-date knowledge), section heading, and section description carefully.
2. Then, look at the provided research papers.
3. Decide which sources that you will use it to write the section.
4. Use as many of references as possible for each section.
5. Return list of title of references research papers.
6. Only return the title, do not include paper's keypoints
7. If the section does not need any references, return an empty list for that section.
</Task>
"""


UPDATE_OUTLINE = """Generate an outline for a research report by adding necessary sections.

<Task>
1.  **Analyze the Context**: Review the user's report proposal (containing title, proposed method, problem statement, motivation, focused research gap, and a web search report), and the user's specified field and domain of research.

2.  **Review the Template**: Examine the provided default outline (if given), which includes a fixed list of headings and their corresponding subheadings.

3.  **Generate a New Outline**: Create a new outline based on a strict combination of the default outline and the user's specific research context. Follow these rules precisely:
    *   **Preserve Everything**: You MUST include every single heading and subheading from the default outline in your new outline.
    *   **No Modifications**: You are strictly forbidden from modifying, rephrasing, or rewording any existing headings and subheadings from the default outline, even if you believe a change would be more suitable.
    *   **No Deletions**: You are strictly forbidden from deleting any headings or subheadings from the default outline. The default structure is the result of a highly tailored process and must be fully preserved.
    *   **Additive Only**: You can ONLY add new headings and new subheadings if they are absolutely essential to address a specific, unique aspect of the user's proposal that is not covered by the default structure.
    *   **Fill Empty Headings**: If any heading in the default outline is provided with an empty list of subheadings, you MUST generate at least two relevant subheadings for that heading based on the user's proposal.

4. **Do not generate "References" and "Appendix"**: it will be input manually by the user
5.  **Format and Language**: Generate the final outline in the user's language.
</Task>
"""

REORDER_OUTLINE = """Your task is to determine the logical order of sections for a research report and return a list of their new positions.

<Role>
You are an expert academic editor. You understand the conventional structure and logical flow of scientific papers, theses, and research reports (e.g., IMRaD structure: Introduction, Methods, Results, and Discussion).
</Role>

<Input>
You will be given a list of strings. Each string is a heading from a research outline. The headings are in an arbitrary, potentially incorrect order. Use the default list of headings as guidance, similar headings usually go together.
</Input>

<Output>
Your response MUST be a single JSON **object** with exactly one key `reorder_outline`.
`reorder_outline` is a JSON list of integers representing the sort priority for each heading.
- The list MUST have the exact same number of items as the input list.
- Lower numbers come first. For example, a heading with priority 1 will appear before a heading with priority 2.
- Do NOT provide any explanation or surrounding text. Your entire response must be ONLY the JSON object, no code fence.
- Shape: {"reorder_outline": [<int>, <int>, ...]}
</Output>

<Task>
1.  **Analyze the Input**: Read the provided list of heading titles.
2.  **Determine Logical Flow**: Based on your expertise in academic writing, determine the most logical sequence for these headings. A standard flow is:
    - Introduction/Preamble (Background, Problem Statement)
    - Literature Review / Theoretical Background / Related Work
    - Methodology / Approach / Materials and Methods
    - Results / Findings / Analysis
    - Discussion / Interpretation
    - Conclusion / Future Work
3.  **Map to Indices**: Create a new list that contains the 1-based indices of the original headings in their new, correct order. For example, if the third item in the input list should come first, the output list will start with `3`.
4.  **Format and Return**: Return ONLY a JSON object of the form {"reorder_outline": [...]} where the array is the list of integers.
</Task>

---
<Example>
<InputExample>
["Introduction", "Results", "Related works", "Theoretical Background"]
</InputExample>

<OutputExample>
{"reorder_outline": [1, 4, 2, 3]}
</OutputExample>

<Reasoning for Example>
The input order is [1, 2, 3, 4]. The correct logical order is "Introduction", "Related Work", "Theoretical Background", "Results". This corresponds to the original indices [1, 2, 4, 3].
</Reasoning for Example>
---
"""

CHOOSE_USER_REFS = """Choose a list of sections that will use the provided research paper as a reference to write.

<Task>
1. Review the user's the report proposal (which contains title, proposed method, problem statement, motivation, focused research gap and a web search report about the research gap with up-to-date knowledge), section heading, and section description carefully.
2. Then, look at the provided research paper.
3. Decide which section will the provided research paper as a reference to write.
4. You **MUST HAVE ATLEAST 1 SECTION** to utilize the provided research paper, since that provided research paper is highly relavant to the research proposal
5. Return list of JSON object with heading of the section (`heading`)
</Task>
"""


CHOOSE_USER_REFS_V2 = """Choose a list of sections that will use the provided research paper as a reference to write.

<Task>
1. Review the user's the report proposal (which contains title, proposed method, problem statement, motivation, focused research gap and a web search report about the research gap with up-to-date knowledge), section heading, and section description carefully.
2. Then, look at the provided research paper.
3. Decide which section will the provided research paper as a reference to write.
4. You **MUST HAVE ATLEAST 1 SECTION** to utilize the provided research paper, since that provided research paper is highly relavant to the research proposal
5. Return a JSON object with list of number of the section with key `usage`
</Task>
"""


CHOOSE_SUBSECTION = """Choose subsections along with theirs references papers that will be used to write.

<Task>
1. Review the user's the report proposal (which contains title, proposed method, problem statement, motivation, focused research gap and a web search report about the research gap with up-to-date knowledge), section heading, and section description carefully.
2. Then, look at the provided research papers and subheadings description.
3. Decide which subsection will the provided research paper as a reference to write.
4. **EACH PROVIDED RESEARCH PAPER MUST HAVE ATLEAST 1 SUBSECTION**, since all the provided research papers is highly relavant to the research proposal
5. A paper can be used in multiple subsections if relevant.
6. Return a JSON object where keys are the Subsection index and values are lists of Paper indexes.
</Task>
"""


UPDATE_OUTLINE_PERCENT = """Update outline's percentage for each section in a research report

<Task>
1. Review the user's report proposal (which contains title, proposed method, problem statement, motivation, focused research gap and a web search report about the research gap with up-to-date knowledge), and user's field and domain of research.
2. Then, look at old outline with theirs heading percentage and new outline with theirs heading percentage
3. Analyze the user's intent and act accordingly
For example:
- If for the same heading but they raise the percentage, which mean they want to focus and write more about that section, your task is to decide which and how much percent other headings need to lower
- If for the same heading but they lower the percentage, which mean they want to focus and write more on other sections, your task is to decide which and how much percent other headings need to raise
- If there are new headings, balance it out as appropriate
4. Generate a list of percentage (from 1 to 99) for each section for the new outline so that it sum up to 100
5. **IMPORTANT**: THE RETURN LIST OF PERCENTAGE MUST NOT HAVE ANY 0
</Task>
"""

GENRATE_SUBHEADINGS = """
You're a PhD professor, you task is to help the student generate subheadings for a section in a report based on final proposal. 
You will be given the final proposal of the report, an example outline (if given) and the section name and description
Return just JSON object
Return in user's language
"""

UPDATE_OUTLINE_CHAT = """Generate a formatted outline for a research report from outline's note and user's note.

<Task>
1.  **Analyze the Context**: Review the user's note (if given) and outline's note. The outline's note may include headings with or without subheadings, but no description and word count

2.  **Generate a formatted Outline**: Create a formatted outline with detail based on a strict combination of the default outline and the user's specific research context.

**IMPORTANT STRUCTURE RULES:**
- If the input outline's note for a section includes subheadings (bullet points, sub-items, or numbered sub-sections), then include a "subheadings" array in the output
- If the input outline's note for a section ONLY has a section title and description WITHOUT subheadings, then set "subheadings" to null (not an empty array)
- DO NOT add subheadings if they were not present in the input outline

**Example with subheadings:**
```
{
  "heading": "Mở đầu",
  "overview": "Chương Mở đầu là phần nền tảng, đặt ra bối cảnh và lý do chọn đề tài nghiên cứu về công bằng môi trường trong đô thị với trọng tâm so sánh đa quốc gia. Chương này bắt đầu bằng việc giới thiệu tổng quan về lĩnh vực nghiên cứu, nêu rõ tầm quan trọng và tính cấp thiết của việc nghiên cứu các vấn đề bất bình đẳng xã hội - môi trường trong các đô thị hiện nay. Tiếp đó, chương trình bày chi tiết về bối cảnh nghiên cứu, xác định khoảng trống kiến thức trong các nghiên cứu trước đây, đặc biệt là thiếu vắng các nghiên cứu so sánh đa quốc gia nhằm làm sáng tỏ các tác động của quy hoạch và chính sách quản trị đô thị. Chương cũng đề cập tới mục tiêu nghiên cứu, câu hỏi nghiên cứu và giả thuyết nhằm hướng đến việc phân tích sâu sắc sự liên quan giữa chính sách quy hoạch đô thị và bất bình đẳng xã hội - môi trường. Phần giới thiệu đối tượng và phạm vi nghiên cứu, cùng với phương pháp tiếp cận tổng quan, giúp làm rõ phạm vi và cách thức thực hiện nghiên cứu. Đồng thời, chương làm nổi bật ý nghĩa khoa học và thực tiễn của đề tài, nhấn mạnh đóng góp mới về lý thuyết và ứng dụng trong bối cảnh toàn cầu hóa và biến đổi khí hậu. Cuối cùng, chương trình bày cấu trúc luận án để người đọc dễ dàng theo dõi, đồng thời tóm tắt các nội dung chính đã trình bày. Các mục phụ như tính cấp thiết của nghiên cứu đa quốc gia và tác động của quy hoạch, chính sách quản trị đô thị cũng được phân tích nhằm củng cố nền tảng lý luận cho các phần nghiên cứu tiếp theo.",
  "word_count": "2000-3000",
  "subheadings": [
    "Giới thiệu chương",
    "Bối cảnh và lý do chọn đề tài",
    "Vấn đề nghiên cứu và khoảng trống (Gap)",
    "Mục tiêu nghiên cứu",
    "Câu hỏi nghiên cứu / Giả thuyết",
    "Đối tượng và phạm vi nghiên cứu",
    "Phương pháp tiếp cận tổng quan",
    "Ý nghĩa khoa học và thực tiễn",
    "Cấu trúc luận án",
    "Tóm tắt chương",
  ]
}
```

**Example WITHOUT subheadings (section only):**
```
{
  "heading": "Introduction",
  "overview": "This chapter presents the context and significance of the era of growth in the digital age, the purpose and scope of the research, the main research question, and the approach.",
  "word_count": "2000-3000",
  "subheadings": null
}
```

3. **Do not generate "References"** as it will be input manually by the user

4.  **Format and Language**: Generate the final outline in the user's language.
</Task>
"""

REORDER_SUBHEADINGS_PRIORITIES_V2 = """Your task is to determine the logical order for a list of subheadings within a research chapter and return their sort priorities, following strict formatting rules.

<Role>
You are an expert academic editor. You understand the logical progression of arguments and evidence within a research chapter.
</Role>

<Input>
1.  `section_heading`: The title of the main chapter these subheadings belong to.
2.  `subheadings`: A JSON list of subheading titles in an arbitrary order.
</Input>

<Output>
Your response MUST be a single JSON **object** with exactly one key `reorder_outline`.
`reorder_outline` is a JSON list of integers representing the sort priority for each subheading.
- The list MUST have the exact same number of items as the input `subheadings` list.
- Lower numbers come first.
- **CRITICAL RULE 1**: The subheading that serves as a chapter introduction (e.g., 'Introduction', 'Overview', 'Giới thiệu chương') MUST be given the lowest priority number (e.g., 1).
- **CRITICAL RULE 2**: The subheading that serves as a chapter summary or conclusion (e.g., 'Summary', 'Conclusion', 'Tóm tắt chương') MUST be given the highest priority number.
- Do NOT provide any explanation or surrounding text. Your entire response must be ONLY the JSON object, no code fence.
- Shape: {"reorder_outline": [<int>, <int>, ...]}
</Output>

<Task>
1.  **Analyze the Context**: Read the main `section_heading` to understand the chapter's purpose.
2.  **Analyze the Subheadings**: Read the provided list of `subheadings`.
3.  **Determine Logical Flow**: Determine the most logical sequence for the subheadings based on the chapter's context.
4.  **Enforce Special Ordering**: Assign the absolute lowest priority to the introductory subheading and the absolute highest priority to the summary subheading. Assign intermediate priorities to all other subheadings based on their logical order.
5.  **Format and Return**: Return ONLY a JSON object of the form {"reorder_outline": [...]} where the array is the list of integers.
</Task>

---
<Example>
<InputExample>
{
  "section_heading": "Analysis of Results",
  "subheadings": [
    "Discussion of Key Finding A",
    "Chapter Introduction",
    "Chapter Summary",
    "Analysis of Finding B"
  ]
}
</InputExample>

<OutputExample>
{"reorder_outline": [3, 1, 4, 2]}
</OutputExample>

<Reasoning for Example>
The logical flow is Intro -> Finding B -> Finding A -> Summary.
- "Chapter Introduction" MUST come first (priority 1).
- "Chapter Summary" MUST come last (priority 4).
- "Analysis of Finding B" comes before "Discussion of Key Finding A" (priorities 2 and 3).
This results in the priorities [3, 1, 4, 2] for the original list.
</Reasoning for Example>
"""