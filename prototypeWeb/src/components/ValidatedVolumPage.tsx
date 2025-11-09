import React from 'react';
import DocumentTextIcon from './icons/DocumentTextIcon';
import GlobeIcon from './icons/GlobeIcon';
import CopyIcon from './icons/CopyIcon';

const ValidatedVolumPage: React.FC = () => {
  const [activeTab, setActiveTab] = React.useState('standard');
  const [originalBlog, setOriginalBlog] = React.useState('');
  const [translatedBlog, setTranslatedBlog] = React.useState('');
  const [copyStatus, setCopyStatus] = React.useState('');

  const handleGenerateAndCopy = () => {
    const promptTemplate = `## ROLE & GOAL
You are a Principal Technical Editor at AWS, a 20-year veteran in cloud architecture with deep expertise in localizing technical content. You are meticulous, exacting, and detail-oriented. Your primary mission is to proofread a provided Vietnamese translation against its English original, identifying all errors in technical accuracy, meaning, and style.

## CONTEXT

- **[Original English Article]**:
${originalBlog}

- **[Current Vietnamese Translation]**:
${translatedBlog}

## CORE DIRECTIVES

1.  **Meaning and Accuracy**: Compare the translation against the original, paragraph by paragraph. Identify and correct any mistranslations, omissions, additions, or stiff, word-for-word phrasing. The final meaning must be technically precise.
2.  **Audience and Tone**: The target audience is beginners in computer science. The tone should be educational, clear, and approachable, but maintain technical authority. Avoid overly casual slang.
3.  **Content Integrity**:
    * **Do Not Add Information**: Never introduce technical information that is not present in the original English text. You may add transitional words to improve flow.
    * **Preserve a Título (Do Not Change)**: Keep the following elements exactly as they appear in the original:
        * Code blocks, API/SDK/CLI names and commands.
        * Parameters, JSON keys, shell commands, and log outputs.
        * File paths, URLs, and AWS region codes (e.g., us-east-1).
        * Numbers and units (e.g., 10 GiB, 200ms). Do not convert them.
        * Links and their anchor text (if the anchor is not a descriptive sentence).

## TERMINOLOGY RULES

1.  **AWS Service Names**: **NEVER** translate AWS service names, product features, or branded terms. They must remain in English with correct capitalization (e.g., *Amazon S3, AWS Lambda, EC2, VPC, Availability Zone, IAM, CloudWatch Logs*).
2.  **General Technical Terms**: Translate common technical terms for clarity, but provide the English original in parentheses upon their **first use** in the article. Be consistent with this translation throughout the text.
    * *Example*: "endpoint" should be translated as "điểm cuối (endpoint)".
    * *Example*: "fault tolerance" should be translated as "chịu lỗi (fault tolerance)".
    * *Example*: "throughput" should be translated as "thông lượng (throughput)".
    * *Example*: "latency" should be translated as "độ trễ (latency)".
    * *Example*: "instance" should be kept in original language.
    * *Example*: "on-premises" should be translated as "cơ sở hạ tầng công nghệ của cơ quan (on-premises)".
    * *Example*: "high availability" should be translated as "khả dụng cao (high availability)".
    * *Example*: "scalability" should be translated as "khả năng mở rộng (scalability)".
    * *Example*: "load balancing" should be translated as "cân bằng tải trọng (load balancing)".
    * *Example*: "auto scaling" should be translated as "tự động điều chỉnh quy mô (auto scaling)".
3.  **Title/Header Check**: Scrutinize the article's title. It must accurately reflect the core topic, use correct terminology, be easy for beginners to understand, and avoid literal, awkward translations.

## STEP-BY-STEP PROCESS

1.  **Initial Scan**: Quickly review the translation to identify key terminology and locate all code blocks, JSON, and CLI commands to ensure they are not altered.
2.  **Title Analysis**: Evaluate the title based on the "Title Check" directive.
3.  **Paragraph-by-Paragraph Review**: Methodically compare each paragraph of the translation to the original. For each identified error, create a report entry using the format below.
4.  **Final Report Generation**: Compile all findings into a single report as specified in the "OUTPUT FORMAT".

## OUTPUT FORMAT

Generate a markdown-formatted "Correction Report" listing every issue found in the order of its appearance. Use the following strict template for each entry:

**A. Correction Report**

* **Paragraph #[Paragraph Number], starts with: “...”**
    * **Current Translation**: [Quote the problematic Vietnamese sentence or phrase]
    * **Original English**: [Quote the corresponding English sentence or phrase]
    * **Suggested Edit**: [Provide the corrected Vietnamese version]
    * **Severity**: [Critical / Major / Minor]
    * **Rationale**: [Briefly explain why the change is needed, referencing issues like: technical inaccuracy, mistranslation, unnatural phrasing, terminology rule violation, or formatting error.]

---
*(Example of a single entry)*

* **Paragraph #3, starts with: “Để bắt đầu, bạn cần một cổng NAT...”**
    * **Current Translation**: Để bắt đầu, bạn cần một cổng NAT trong mạng riêng ảo của mình.
    * **Original English**: To get started, you will need a NAT Gateway in your VPC.
    * **Suggested Edit**: Để bắt đầu, bạn sẽ cần một NAT Gateway trong VPC của mình.
    * **Severity**: Major
    * **Rationale**: Terminology rule violation. "VPC" is a branded AWS term and should not be translated as "mạng riêng ảo". "NAT Gateway" should also remain untranslated.`;

    navigator.clipboard.writeText(promptTemplate.trim()).then(() => {
      setCopyStatus('Copied!');
      setTimeout(() => setCopyStatus(''), 2000);
    }, () => {
      setCopyStatus('Failed to copy!');
      setTimeout(() => setCopyStatus(''), 2000);
    });
  };

  return (
    <div className="flex flex-col flex-grow">
      <div className="bg-white pt-8">
        <div className="container mx-auto text-center">
          <h1 className="text-3xl font-bold text-[#232F3E]">AWS Blog Translation Prompt Generator</h1>
          <p className="mt-2 text-md text-gray-600">Professional Technical Content Localization Tool</p>
        </div>
      </div>

      <main className="flex-grow bg-gray-100 p-4 sm:p-6 lg:p-8">
        <div className="container mx-auto">
          <div className="grid grid-cols-1 gap-8 md:grid-cols-2">
            {/* Left Card */}
            <div className="flex flex-col rounded-lg bg-white shadow-md">
              <div className="border-b border-gray-200 border-t-4 border-t-orange-500 p-4">
                <h2 className="flex items-center text-lg font-semibold text-gray-800">
                  <DocumentTextIcon className="mr-3 h-6 w-6 text-gray-500" />
                  Original Blog (English)
                </h2>
              </div>
              <div className="flex-grow p-4">
                <textarea
                  className="h-96 w-full resize-none rounded-md border border-gray-300 p-3 text-sm text-gray-800 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                  placeholder="Paste the original English blog content here..."
                  value={originalBlog}
                  onChange={(e) => setOriginalBlog(e.target.value)}
                />
              </div>
            </div>

            {/* Right Card */}
            <div className="flex flex-col rounded-lg bg-white shadow-md">
              <div className="border-b border-gray-200 border-t-4 border-t-cyan-500 p-4">
                <h2 className="flex items-center text-lg font-semibold text-gray-800">
                  <GlobeIcon className="mr-3 h-6 w-6 text-gray-500" />
                  Translated Blog (Vietnamese)
                </h2>
              </div>
              <div className="flex-grow p-4">
                <textarea
                  className="h-96 w-full resize-none rounded-md border border-gray-300 p-3 text-sm text-gray-800 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                  placeholder="Paste the Vietnamese translated blog content here..."
                  value={translatedBlog}
                  onChange={(e) => setTranslatedBlog(e.target.value)}
                />
              </div>
            </div>
          </div>
          <div className="mt-8 text-center">
            <button
              type="button"
              onClick={handleGenerateAndCopy}
              className="inline-flex items-center rounded-md bg-[#232F3E] px-6 py-3 font-medium text-white shadow-lg transition-transform duration-300 hover:scale-105 hover:bg-[#34495E]"
            >
              <CopyIcon className="mr-3 h-5 w-5" />
              Validate
            </button>
            {copyStatus && (
              <p className="mt-2 text-sm font-medium text-green-600 transition-opacity duration-300">
                {copyStatus}
              </p>
            )}
          </div>
        </div>
      </main>
    </div>
  );
};

export default ValidatedVolumPage;