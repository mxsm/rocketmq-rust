$(document).ready(function () {
    var mmSkin = "dark"
    var mjsTheme = {
        "air": "air",
        "aqua": "aqua",
        "contrast": "contrast",
        "dark": "dark",
        "default": "default",
        "dirt": "dirt",
        "mint": "mint",
        "neon": "dark",
        "plum": "dark",
        "sunrise": "sunrise"
    }[mmSkin]
    mermaid.initialize({
        startOnLoad: false,
        theme: mjsTheme
    })
    mermaid.init({
        theme: mjsTheme
    }, '.language-mermaid');
});