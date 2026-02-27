console.log("test");

function initItemLinks(item) {
    for (const link of item.querySelectorAll(".replink")) {
        const end = document.createElement("span");
        link.appendChild(end);

        let popover = null;
        let pinned = false;
        let mouseDownStart = null;
        let clickTimer = null;

        link.onmousedown = (e) => {
            mouseDownStart = Date.now();
            setTimeout(() => {
                if (popover) {
                    popover.classList.add("pinned");
                    pinned = true;

                    const close = document.createElement("button");
                    close.textContent = "X";
                    close.onclick = () => {
                        popover.remove();
                        popover = null;
                        pinned = false;
            }

            popover.querySelector(".header")?.appendChild(close);
                }
            }, 256);
        }
        link.onclick = (e) => {
            clearTimeout(clickTimer);
            if (mouseDownStart) {
                const duration = Date.now() - mouseDownStart;
                if (duration >= 256) {
                    e.preventDefault();
                    return;
                }
            }
        }

        link.onmouseover = () => {
            if (popover) {
                return;
            }

            const m = link.href.match(/#(.*)$/)
            if (!m) return;

            const id = m[1];
            const target = document.getElementById(id);
            if (!target) return;

            link.style.anchorName = `--${item.id}-${id}`;

            link.title = "Click and hold to pin"; // remove native popover title

            popover = target.cloneNode(true);
            popover.classList.add("popover");
            popover.style.position = "absolute";
            popover.style.positionAnchor = link.style.anchorName;
            popover.style.positionArea = end.offsetLeft > window.innerWidth/2
                ? "top span-left"
                : "top span-right";

            popover.querySelector(".reply-links")?.remove();

            document.body.appendChild(popover);
        };

        link.onmouseout = () => {
            if (popover && !pinned) {
                popover.remove();
                popover = null;
                pinned = false;
            }
        };
    }
}

const observer = new MutationObserver(e => {
    for (const record of e) {
        for (const node of record.addedNodes || []) {
            if (node.nodeType != Node.ELEMENT_NODE) continue;
            if (node.tagName == "FOOTER")  {
                observer.disconnect();
                break;
            }
            if (!node.classList.contains("item")) continue;
            initItemLinks(node);
        }
    }
});

observer.observe(document.querySelector("body"), {
    childList: true,
})
