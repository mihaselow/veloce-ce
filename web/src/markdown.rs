//! Lightweight Markdown → HTML for agent chat bubbles.

use pulldown_cmark::{html, Event, Options, Parser};

/// Convert Markdown to HTML. Raw HTML in the source is treated as text (escaped).
pub fn markdown_to_html(markdown: &str) -> String {
    let mut options = Options::empty();
    options.insert(Options::ENABLE_STRIKETHROUGH);
    options.insert(Options::ENABLE_TABLES);
    options.insert(Options::ENABLE_TASKLISTS);
    let parser = Parser::new_ext(markdown, options).map(|event| match event {
        // Never pass through raw HTML from the model.
        Event::Html(s) | Event::InlineHtml(s) => Event::Text(s),
        other => other,
    });
    let mut html_out = String::new();
    html::push_html(&mut html_out, parser);
    html_out
}

#[cfg(test)]
mod tests {
    use super::markdown_to_html;

    #[test]
    fn renders_bold_and_heading() {
        let html = markdown_to_html("### Tools\n\nUse **load cantilever**.");
        assert!(html.contains("<h3>"));
        assert!(html.contains("<strong>load cantilever</strong>"));
    }

    #[test]
    fn renders_table() {
        let html = markdown_to_html("| Host | IP |\n| --- | --- |\n| w1 | 10.0.0.1 |");
        assert!(html.contains("<table>"));
        assert!(html.contains("<th>"));
        assert!(html.contains("w1"));
    }

    #[test]
    fn escapes_raw_html_tags() {
        let html = markdown_to_html("a <script>alert(1)</script> b");
        assert!(!html.contains("<script>"), "raw script tag leaked: {html}");
        assert!(html.contains("&lt;script&gt;") || html.contains("alert(1)"));
    }
}
