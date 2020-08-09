$(function() {
  "use strict"; // Start of use strict

  // Smooth scrolling using jQuery easing
  $('a.js-scroll-trigger[href*="#"]:not([href="#"])').click(function() {
    if (location.pathname.replace(/^\//, '') == this.pathname.replace(/^\//, '') && location.hostname == this.hostname) {
      var target = $(this.hash);
      target = target.length ? target : $('[name=' + this.hash.slice(1) + ']');
      if (target.length) {
        $('html, body').animate({
          scrollTop: (target.offset().top - 56)
        }, 1000, "easeInOutExpo");
        return false;
      }
    }
  });

  $(".navbar-burger").click(function() {
      $(".navbar-burger").toggleClass("is-active");
      $(".navbar-menu").toggleClass("is-active");
  });

  function decrypt(text, salt, shift) {
    var result = [];
    for(var i=0; i<text.length; ++i) {
      result.push(String.fromCharCode(text.charCodeAt(i) ^ salt.charCodeAt((i + shift) % salt.length)));
    }
    return result.join('');
  }

  $(".encrypted-text").each(function() {
    var text = $(this).data("encText");
    var salt = $(this).data("encTextSalt");
    var shift = $(this).data("encTextShift");
    $(this).text(decrypt(text, salt, shift));
  });

  $(".encrypted-href").each(function() {
    var text = $(this).data("encHref");
    var salt = $(this).data("encHrefSalt");
    var shift = $(this).data("encHrefShift");
    $(this).attr("href", decrypt(text, salt, shift));
  });

  function showLazyElement(element) {
    if ($(element).hasClass('lazy-blur')) {
      if ($(element).hasClass('lazy-slow')) {
        $(element).addClass('lazy-blur-slow-fade');
      } else {
        $(element).addClass('lazy-blur-fade');
      }

      $(element).removeClass('lazy-blur');
    }
  };

  var lazyArgs = {
    afterLoad: showLazyElement,
  };

  $('video.lazy-blur').each(function() {
    this.onloadeddata = showLazyElement();

    if (!this.paused) {
      showLazyElement();
    }
  });

  $('.lazy-hide').removeClass("lazy-hide");
  $('.lazy').Lazy(lazyArgs);
});

window.dataLayer = window.dataLayer || [];
function gtag(){dataLayer.push(arguments);}
gtag('js', new Date());

gtag('config', 'UA-174941702-1');
