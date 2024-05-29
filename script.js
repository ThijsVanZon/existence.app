document.addEventListener("DOMContentLoaded", function() {
    const decades = document.querySelectorAll(".decade");
  
    decades.forEach((decade, index) => {
      decade.addEventListener("mouseenter", () => {
        highlightDecade(index);
      });
      decade.addEventListener("mouseleave", () => {
        resetDecadeHighlighting();
      });
    });
  
    function highlightDecade(index) {
      if (index > 0) {
        decades[index - 1].classList.add("highlight");
      }
      if (index < decades.length - 1) {
        decades[index + 1].classList.add("highlight");
      }
      decades[index].classList.add("highlight");
    }
  
    function resetDecadeHighlighting() {
      decades.forEach(decade => {
        decade.classList.remove("highlight");
      });
    }
  });
  