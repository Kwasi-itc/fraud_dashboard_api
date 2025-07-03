When writing README.md, you **MUST** follow these principles:

## **1\. Core Principles**

* **Clarity & Conciseness:** The README **MUST** provide a clear, unambiguous, and concise overview of the project. Avoid unnecessary jargon; if technical terms are essential, they **MUST** be defined upon first use.  
* **Accessibility:** The README **MUST** be easily consumable by a diverse audience, including developers, non-technical users, and automated parsers. Use simple language and standard Markdown.  
* **Maintainability:** The structure **MUST** facilitate straightforward updates and automated generation by Aider. Avoid complex formatting that hinders programmatic parsing.  
* **Gateway, Not Encyclopedia:** The README.md **MUST** serve as an entry point, providing *only* essential information. Detailed documentation, API references, or in-depth guides **MUST** reside in separate docs/ directories or external documentation sites, with clear, direct links from the README.  
* **Visual Appeal:** Utilize standard Markdown formatting, and where appropriate, incorporate visual elements (e.g., badges, screenshots, GIFs) to enhance understanding and engagement. Visuals **MUST** directly support the text.

## **2\. File Structure & Naming**

* The primary documentation file **MUST** be named README.md and **MUST** be located at the root of the project repository. No alternative filenames (e.g., ReadMe.md, readme.txt) are permitted.

## **3\. Markdown Formatting Standards**

Aider will strictly adhere to **CommonMark** specifications for Markdown rendering to ensure consistent display across all platforms (e.g., GitHub, GitLab, Bitbucket).

* **Headings:**  
  * **MUST** use **ATX headings** (\#, \#\#, \#\#\#, \#\#\#\#, \#\#\#\#\#, \#\#\#\#\#\#). Setext headings (underlined) are **PROHIBITED**.  
  * The project title **MUST** use a single hash (\# Project Name). There **MUST NOT** be a trailing hash.  
  * Major sections **MUST** use double hashes (\#\# Section Title).  
  * Sub-sections **MUST** use triple hashes (\#\#\# Sub-section Title). Further sub-sections may use \#\#\#\# etc., but strive to limit depth to \#\#\#.  
  * **MUST** maintain a logical heading hierarchy. Skipped levels (e.g., \#\# directly followed by \#\#\#\#) are **PROHIBITED**.  
* **Emphasis:**  
  * Use single asterisks for *italics* (\*text\*). Example: This is \*important\* information.  
  * Use double asterisks for **bold** (\*\*text\*\*). Example: The \*\*primary\*\* goal is to...  
  * **MUST** use emphasis sparingly to maintain impact.  
* **Lists:**  
  * **Unordered Lists:** Use hyphens (-) for list item markers. Example: \- Item One.  
  * **Ordered Lists:** Use 1\. for all list items. The rendering engine will automatically number them. Example: 1\. First step then 1\. Second step.  
  * **Nesting:** Maintain consistent indentation for nested lists (2 or 4 spaces, but be consistent throughout the document).  
* **Code Blocks:**  
  * Use triple backticks (\`\`\`) for multi-line code blocks.  
  * **ALWAYS** specify the language for syntax highlighting immediately after the opening backticks (e.g., python\`, javascript, \`\`\`\`bash, \`\`\`\`json\`).  
  * For short inline code or commands, use single backticks (\`inline code\`). Example: Run the install.sh script.  
* **Links:**  
  * **External Links:** Use \[Link Text\](URL) format. Example: \[Google\](https://www.google.com).  
  * **Internal Links (to headings):** Use \[Section Title\](\#section-title) format. The section ID **MUST** be lowercase, space-replaced with hyphens. Example: \[Installation\](\#installation).  
  * **File Links (within repo):** Use relative paths. Example: \[CONTRIBUTING.md\](CONTRIBUTING.md).  
* **Images:**  
  * Use \!\[Alt text\](path/to/image.png) format.  
  * **ALWAYS** provide meaningful and descriptive **alt text** for accessibility. Example: \!\[Screenshot of the main dashboard\](assets/dashboard.png).  
* **Horizontal Rule:** Use three hyphens (---) for a horizontal rule to visually separate major sections, but use sparingly.

## **4\. Standard Sections & Order**

Aider will generate READMEs with the following standard sections, in this precise recommended order. Optional sections **MUST** only be included if relevant, populated content exists.

### **4.1. Required Sections**

1. **\# Project Title**  
   * The concise and descriptive name of the project.  
   * **MUST** be immediately followed by relevant **badges** (e.g., build status, license, version, contributors). These badges **MUST** be generated from project metadata where available. Example: \!\[License: MIT\](https://img.shields.io/badge/License-MIT-yellow.svg) \!\[Build Status\](https://github.com/user/repo/workflows/CI/badge.svg).  
2. **\#\# Description**  
   * A concise, one-to-two-sentence summary of what the project is and what it does, serving as an elevator pitch.  
   * Followed by a slightly longer paragraph (2-4 sentences) expanding on its primary purpose, target audience, and key benefits.  
3. **\#\# Table of Contents** (Conditional Mandatory)  
   * **MUST** be included if the README has 5 or more \#\# (major) sections.  
   * A bulleted list of internal links to all major \#\# sections, explicitly listed for user navigation. Example:  
     \- \[Description\](\#description)  
     \- \[Features\](\#features)  
     \- \[Installation\](\#installation)

4. **\#\# Features**  
   * A concise, bulleted list highlighting the key functionalities, capabilities, or unique selling points of the project. Each feature **MUST** be a single line.  
5. **\#\# Installation**  
   * Step-by-step, clear, and executable instructions on how to set up the project locally.  
   * **MUST** include any prerequisites (e.g., specific software versions like Node.js 18+, Python 3.9+, Docker).  
   * **MUST** provide explicit command-line instructions for cloning the repository, installing dependencies, and building if necessary. Example:  
     \# Clone the repository  
     git clone \[https://github.com/your-org/your-project.git\](https://github.com/your-org/your-project.git)  
     cd your-project

     \# Install dependencies  
     npm install

     \# Build (if applicable)  
     npm run build

6. **\#\# Usage**  
   * Clear instructions on how to run the project, use its main features, or integrate it into another project.  
   * **MUST** include concise, copy-pasteable code examples (for libraries/APIs) or command-line examples (for applications) demonstrating typical workflows or common use cases. Example (for a CLI app):  
     \# Run the application  
     ./your\_app\_name \--input data.txt \--output result.json

     \# Get help  
     ./your\_app\_name \--help

7. **\#\# Contributing**  
   * A brief, welcoming invitation to contribute.  
   * **MUST** include a direct link to a dedicated CONTRIBUTING.md file if one exists. If not, provide minimal, actionable steps (e.g., "Fork the repository, create a feature branch, and submit a pull request").  
8. **\#\# License**  
   * A clear statement of the project's open-source license (e.g., Distributed under the MIT License.).  
   * **MUST** include a direct link to the LICENSE file in the repository. Example: See \[LICENSE\](LICENSE) for more information.

### **4.2. Recommended Optional Sections**

Include these sections **ONLY IF** they are directly relevant and add significant value to the project's understanding. They **MUST NOT** be included as empty placeholders.

* **\#\# Project Status**  
  * Indicate the current development status (e.g., "Under active development," "Beta," "Maintenance mode," "Archived").  
* **\#\# Technologies Used**  
  * A concise list of the main programming languages, frameworks, significant libraries, and key tools employed. Use bullet points. Example:  
    \- \*\*Frontend:\*\* React, Tailwind CSS  
    \- \*\*Backend:\*\* Node.js, Express.js  
    \- \*\*Database:\*\* PostgreSQL  
    \- \*\*Tools:\*\* Docker, Webpack

* **\#\# Examples** or **\#\# Demos**  
  * More extensive code snippets, links to live demos/deployments, or animated GIFs/screenshots showcasing complex features.  
* **\#\# API Reference** (For API projects)  
  * A brief overview of key endpoints, authentication methods, and basic request/response formats.  
  * **MUST** link to more comprehensive, external API documentation (e.g., Swagger UI, Postman collection, a docs/api.md file).  
* **\#\# Testing**  
  * Instructions on how to run the project's automated tests. Provide clear command-line examples.  
* **\#\# Roadmap**  
  * A high-level overview of planned features, future directions, or major milestones. Keep this concise and strategic.  
* **\#\# Support** or **\#\# Contact**  
  * Clear instructions on how users can get help, report issues, or contact maintainers (e.g., GitHub Issues link, Discord server invite, support email).  
* **\#\# Acknowledgements**  
  * Credit to individuals, organizations, other projects, or resources that inspired or significantly contributed to the project.

## **5\. README vs. Comprehensive Documentation: Defining the Scope**

The README.md **MUST** serve as the project's high-level gateway and essential overview. It is **NOT** a substitute for comprehensive documentation.

Content that **MUST GENERALLY BE EXCLUDED** from the README.md to prevent information overload and maintain focus:

* Extensive API reference documentation (link to dedicated docs instead).  
* Highly detailed build instructions for development environments (these belong in CONTRIBUTING.md or a development guide).  
* Deep architectural overviews or intricate internal workings.  
* Long, unformatted paragraphs of text exceeding 3-4 sentences.  
* Excessive technical details not immediately necessary for a user to understand the project's core purpose or get started.

Longer, more detailed documentation **MUST** reside in dedicated documentation platforms, project wikis, or separate Markdown files within a docs/ directory.

## **6\. Tailoring READMEs for Diverse Project Types**

Aider will adapt the content and emphasis of the README based on the specific type of project being generated, while strictly adhering to the general conventions.

### **6.1. Applications (GUI/CLI)**

* **Emphasis:** End-user experience, quick setup, direct usage.  
* **Visuals:** Prioritize screenshots and GIFs demonstrating the UI or CLI interaction flow.  
* **Sections:** Focus on clear "How to Run" steps, common user scenarios, and user-level configuration.

### **6.2. Libraries**

* **Emphasis:** Developer integration, dependency management, minimal code examples.  
* **Visuals:** Code snippets demonstrating core API usage for integration.  
* **Sections:** Highlight "Installation" via package managers, "Minimal Integration" examples, and a high-level API overview (with links to full API docs).

### **6.3. APIs (Application Programming Interfaces)**

* **Emphasis:** API contract, authentication, request/response cycles, error handling, versioning.  
* **Visuals:** curl examples, JSON request/response samples, and potentially sequence diagrams.  
* **Sections:** Include "Authentication," "Endpoints," "Versioning," "Error Codes," and notes on "Rate Limiting" or "Caching."

## **7\. Aider Generation Specifics**

* Aider will strictly prioritize generating markdown that adheres precisely to these conventions.  
* Aider may use clearly marked placeholders (e.g., \<\!-- TODO: Add detailed example here \--\>) for sections requiring specific user input or for optional sections that cannot be fully populated automatically.  
* Aider will strive to keep the initial README concise and focused on immediate utility, encouraging users to expand upon relevant sections where appropriate.