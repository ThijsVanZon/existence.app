document.addEventListener("DOMContentLoaded", function() {
    const mainDecades = document.querySelectorAll(".main-decade-container");
    const subDecades = document.querySelectorAll(".sub-decade-container");

    mainDecades.forEach((decade, index) => {
        decade.addEventListener("mouseenter", () => {
            highlightDecade(index);
        });
    });

    function highlightDecade(index) {
        resetDecadeHighlighting();
        mainDecades[index].classList.add("highlight");

        // Highlight the nearest two sub-decades
        if (index > 0) {
            subDecades[index].classList.add("highlight");
        }
        if (index < mainDecades.length - 1) {
            subDecades[index + 1].classList.add("highlight");
        } else {
            // Edge case for the last main decade (2050)
            subDecades[index + 1].classList.add("highlight");
        }

        // Special case for the first main decade (2000)
        if (index === 0) {
            subDecades[0].classList.add("highlight");
        }
    }

    function resetDecadeHighlighting() {
        mainDecades.forEach(mainDecade => {
            mainDecade.classList.remove("highlight");
        });
        subDecades.forEach(subDecade => {
            subDecade.classList.remove("highlight");
        });
    }
});
