let popover = null;
let highlighted = null;

window.onkeydown = e => {
    if (e.keyCode == 27) { // escape key
        popover?.remove();
        popover = null;
    }
};
function initItemLinks(item) {
    for (const link of item.querySelectorAll(".replink")) {
        const end = document.createElement("span");

        link.title = ""; // remove native popover title
        link.appendChild(end);

        link.onpointerover = () => {
            popover?.remove();
            highlighted?.classList?.remove("highlighted");

            const m = link.href.match(/#(.*)$/)
            if (!m) return;

            const id = m[1];
            const target = document.getElementById(id);
            if (!target) return;

            highlighted = target;

            popover = target.cloneNode(true);
            popover.classList.add("popover");

            target.classList.add("highlighted");

            const t_rect = target.getBoundingClientRect();
            if (t_rect.top > 0 && t_rect.bottom < innerHeight) {
                return;
            }

            // Note: popover must be attached to the node tree before
            // the bounding rect can be computed.
            document.body.appendChild(popover);

            const link_rect = link.getBoundingClientRect();
            const screen_offset = link_rect.top/window.innerHeight;

            popover.style.position = "absolute";

            if (link_rect.right < window.innerWidth/2)
                popover.style.left =  Math.floor(link_rect.left) + "px";
            else
                popover.style.right =  Math.ceil(innerWidth - link_rect.right) + "px";

            // Note: popover left or right position must be computed first
            // before getting the bounding rect. This is because the node's
            // bounding rect will be adjust if the node is too large too fit
            // in the screen given the right or left position.
            const p_rect = popover.getBoundingClientRect();

            if (screen_offset < 0.5) 
                popover.style.top = Math.floor(link_rect.bottom + scrollY) +"px";
            else
                popover.style.top = Math.floor(link_rect.top + -p_rect.height + scrollY) +"px";


            popover.querySelector(".reply-links")?.remove();

        };

        link.onmouseout = () => {
            highlighted?.classList?.remove("highlighted");
            highlighted = null;

            popover?.remove();
            popover = null;
        };
    }
}

// https://stackoverflow.com/a/4819886
function isTouchDevice() {
  return (('ontouchstart' in window) ||
     (navigator.maxTouchPoints > 0) ||
     (navigator.msMaxTouchPoints > 0));
}


if (!isTouchDevice()) {
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
}
