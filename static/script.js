document.addEventListener("DOMContentLoaded", function () {
    const mainDecades = document.querySelectorAll(".main-decade-container");
    const subDecades = document.querySelectorAll(".sub-decade-container");

    mainDecades.forEach((decade, index) => {
        decade.addEventListener("mouseenter", () => {
            highlightDecade(index);
        });

        const decadeLink = decade.querySelector("a");
        if (decadeLink) {
            decadeLink.addEventListener("focus", () => {
                highlightDecade(index);
            });

            decadeLink.addEventListener("blur", resetDecadeHighlighting);
        }
    });

    function highlightDecade(index) {
        resetDecadeHighlighting();
        mainDecades[index].classList.add("highlight");

        if (index > 0 && subDecades[index]) {
            subDecades[index].classList.add("highlight");
        }

        if (subDecades[index + 1]) {
            subDecades[index + 1].classList.add("highlight");
        }

        if (index === 0 && subDecades[0]) {
            subDecades[0].classList.add("highlight");
        }
    }

    function resetDecadeHighlighting() {
        mainDecades.forEach((mainDecade) => {
            mainDecade.classList.remove("highlight");
        });

        subDecades.forEach((subDecade) => {
            subDecade.classList.remove("highlight");
        });
    }
});
