// a script that addes back and forward history buttons
// because going back and forth on mobile devices is a pain (in the rear end)

window.onload = () => {
    const customNav = document.createElement("div");
    customNav.innerHTML = `
     <div id="custom-nav">
      <button class="back">go back</button>
      <span class="spacer"> </span>
      <button class="forward">go forth</button>
    </div>
    `;
    document.body.appendChild(customNav);

    const backButton = customNav.querySelector("button.back");
    const forwardButton = customNav.querySelector("button.forward");

    backButton.onclick = e => {
        e.preventDefault();
        history.back();
    };

    forwardButton.onclick = e => {
        e.preventDefault();
        history.forward();
    };

};
