document.addEventListener('DOMContentLoaded', function() {
    
    // --- Tab Component Logic ---
    const tabContainers = document.querySelectorAll('.tab-container');

    tabContainers.forEach(container => {
        const buttons = container.querySelectorAll('.tab-button');
        const contents = container.querySelectorAll('.tab-content');

        buttons.forEach(button => {
            button.addEventListener('click', () => {
                const tabName = button.getAttribute('data-tab');
                
                buttons.forEach(btn => btn.classList.remove('active'));
                contents.forEach(content => content.classList.remove('active'));
                
                button.classList.add('active');
                container.querySelector(`.tab-content[data-tab="${tabName}"]`).classList.add('active');
            });
        });
    });

    
    // --- Active Nav on Scroll Logic ---
    const navLinks = document.querySelectorAll('.nav-link');
    const sections = document.querySelectorAll('.card[id]');
    
    if (navLinks.length > 0 && sections.length > 0) {
        const observer = new IntersectionObserver((entries) => {
            entries.forEach(entry => {
                if (entry.isIntersecting) {
                    const id = entry.target.getAttribute('id');
                    const activeLink = document.querySelector(`.nav-link[href="#${id}"]`);
                    
                    navLinks.forEach(link => link.classList.remove('active'));
                    
                    if (activeLink) {
                        activeLink.classList.add('active');
                    }
                }
            });
        }, {
            threshold: 0.5 
        });

        sections.forEach(section => {
            observer.observe(section);
        });
    }


    // --- Copy-to-Clipboard Button Logic ---
    const allCopyButtons = document.querySelectorAll('.copy-button');

    allCopyButtons.forEach(button => {
        button.addEventListener('click', (e) => {
            const wrapper = button.closest('.code-block-wrapper');
            const code = wrapper.querySelector('pre code');
            const textToCopy = code.innerText;

            // Get the icon spans
            const copyIcon = button.querySelector('.copy-icon');
            const copiedIcon = button.querySelector('.copied-icon');

            // Copy to clipboard
            navigator.clipboard.writeText(textToCopy).then(() => {
                // Success feedback
                button.classList.add('copied');
                copyIcon.style.display = 'none';
                copiedIcon.style.display = 'inline-block';

                // Revert after 2 seconds
                setTimeout(() => {
                    button.classList.remove('copied');
                    copyIcon.style.display = 'inline-block';
                    copiedIcon.style.display = 'none';
                }, 2000);
            }).catch(err => {
                console.error('Failed to copy text: ', err);
            });
        });
    });

});